"""
EU Data Act portal client for eu-data-act.drivesomethinggreater.com.

Ports the evcc Go implementation (vehicle/vw/eudataact/api.go +
vehicle/vag/vwidentity/endpoint.go) to Python.  Authentication uses the
VW group identity OIDC flow; the resulting session cookie (not OAuth tokens)
is what the portal accepts for all subsequent data API calls.
"""

import io
import json
import logging
import re
import secrets
import zipfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, quote

import requests

LOG = logging.getLogger("VWsFriend")

BASE_URL = "https://eu-data-act.drivesomethinggreater.com"
REDIRECT_URI = BASE_URL + "/login"
SCOPE = "openid cars profile"
IDENTITY_BASE = "https://identity.vwgroup.io"
AUTH_URL = IDENTITY_BASE + "/oidc/v1/authorize"

# OIDC client IDs per brand (from evcc types.go)
BRAND_CLIENT_IDS: dict[str, tuple[str, str]] = {
    "volkswagen": ("9b58543e-1c15-4193-91d5-8a14145bebb0@apps_vw-dilab_com", "VOLKSWAGEN_PASSENGER_CARS"),
    "audi": ("cc29b87a-5e9a-4362-aecf-5adea6b01bbb@apps_vw-dilab_com", "AUDI"),
    "skoda": ("3ea88bf9-1d4e-4a68-b3ad-4098c1f1d246@apps_vw-dilab_com", "SKODA"),
    "seat": ("f85e5b69-e3b2-43aa-9c0d-1b7d0e0b576f@apps_vw-dilab_com", "SEAT"),
    "cupra": ("f85e5b69-e3b2-43aa-9c0d-1b7d0e0b576f@apps_vw-dilab_com", "CUPRA"),
}


class _FormParser(HTMLParser):
    """Minimal HTML form input extractor."""

    def __init__(self, form_selector: str) -> None:
        super().__init__()
        self._form_selector = form_selector  # e.g. "emailPasswordForm"
        self._in_target = False
        self.action: str = ""
        self.inputs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "form":
            form_id = attr.get("id", "")
            form_class = attr.get("class", "")
            if self._form_selector in (form_id, form_class):
                self._in_target = True
                self.action = attr.get("action", "")
        if self._in_target and tag == "input":
            name = attr.get("name")
            if name:
                self.inputs[name] = attr.get("value", "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self._in_target = False


def _parse_form(html: str, form_id: str) -> tuple[str, dict[str, str]]:
    """Extract action URL and input values from an HTML form by id."""
    parser = _FormParser(form_id)
    parser.feed(html)
    if not parser.action and not parser.inputs:
        raise ValueError(f"form '{form_id}' not found in page")
    return parser.action, parser.inputs


def _parse_credentials_page(html: str) -> dict[str, Any]:
    """
    Extract the window._IDK JS object from the VW identity credentials page.
    Returns a dict with keys: csrf_token, templateModel.{hmac, relayState,
    postAction, identifierUrl, error}.
    """
    match = re.search(r"(?s)window\._IDK\s*=\s*(.*?)[;<]", html)
    if not match:
        raise ValueError("IDK block not found on credentials page")
    raw = match.group(1)
    # Convert JS object literal to JSON: quote unquoted keys, swap single→double quotes
    raw = raw.replace("'", '"')
    raw = re.sub(r'\s(\w+)\s*:', r' "\1":', raw)
    raw = re.sub(r"(?s),\s*}", "}", raw)
    return json.loads(raw)


class EUDataActAPI:
    """
    Portal client.  Authenticate once; session cookie persists for data calls.
    Re-authenticates automatically on 401/403.
    """

    def __init__(self, username: str, password: str, brand: str = "volkswagen") -> None:
        brand_key = brand.lower()
        if brand_key not in BRAND_CLIENT_IDS:
            raise ValueError(f"unknown brand '{brand}'; choose from {list(BRAND_CLIENT_IDS)}")
        self._client_id, self._state_suffix = BRAND_CLIENT_IDS[brand_key]
        self._username = username
        self._password = password
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0 VWsFriend/1.0"})

    def login(self) -> None:
        """Perform OIDC auth-code flow against VW identity; store session cookie."""
        s = self._session
        s.cookies.clear()

        # Prime the portal session (best effort)
        try:
            s.get(BASE_URL + "/", timeout=15, allow_redirects=True)
        except Exception:  # pylint: disable=broad-exception-caught  # nosec B110
            pass

        # Start OIDC authorize
        nonce = secrets.token_urlsafe(32)
        params = {
            "client_id": self._client_id,
            "response_type": "code",
            "scope": SCOPE,
            "state": f"de__en__{self._state_suffix}",
            "redirect_uri": REDIRECT_URI,
            "prompt": "login",
            "nonce": nonce,
        }
        resp = s.get(AUTH_URL + "?" + urlencode(params), timeout=15, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        # Try legacy flow (emailPasswordForm) first, then new flow
        try:
            action, inputs = _parse_form(html, "emailPasswordForm")
            self._login_legacy(s, action, inputs)
        except ValueError:
            self._login_new(s, html)

    def _login_legacy(self, s: requests.Session, action: str, inputs: dict[str, str]) -> None:
        """Legacy VW identity two-step form flow."""
        identifier_url = IDENTITY_BASE + action

        # Step 1: submit email
        resp = s.post(identifier_url, data={
            "_csrf": inputs.get("_csrf", ""),
            "relayState": inputs.get("relayState", ""),
            "hmac": inputs.get("hmac", ""),
            "email": self._username,
        }, timeout=15, allow_redirects=True)
        resp.raise_for_status()

        idk = _parse_credentials_page(resp.text)
        if idk.get("templateModel", {}).get("error"):
            raise RuntimeError(idk["templateModel"]["error"])

        tm = idk.get("templateModel", {})
        csrf = idk.get("csrf_token", "")
        authenticate_url = identifier_url.replace(
            tm.get("identifierUrl", ""), tm.get("postAction", "")
        )

        # Step 2: submit password — follow redirect chain back to portal
        resp = s.post(authenticate_url, data={
            "_csrf": csrf,
            "relayState": tm.get("relayState", ""),
            "hmac": tm.get("hmac", ""),
            "email": self._username,
            "password": self._password,
        }, timeout=15, allow_redirects=False)

        # Follow HTTPS redirects; stop at non-HTTPS (portal callback)
        resp = self._follow_redirects(s, resp)

        # Handle optional marketing consent interstitial
        resp = self._skip_marketing_consent(s, resp)

        final_url = resp.url
        if resp.status_code >= 400:
            raise RuntimeError(f"Login failed: {resp.status_code} {resp.reason}")
        parsed = urlparse(final_url)
        if parsed.netloc != urlparse(BASE_URL).netloc:
            if "signin-service" in parsed.path or "/consent" in parsed.path or "/error" in parsed.path:
                raise RuntimeError(
                    f"Login did not complete — open the portal and confirm consent: {final_url}"
                )
            raise RuntimeError(f"Login did not complete: landed on unexpected host {parsed.netloc}")

    def _login_new(self, s: requests.Session, html: str) -> None:
        """New VW identity single-step flow (state-based)."""
        m = re.search(r'<input[^>]+name=["\']state["\'][^>]+value=["\']([^"\']+)["\']', html)
        if not m:
            raise ValueError("state parameter not found in new login form")
        state = m.group(1)

        resp = s.post(
            f"{IDENTITY_BASE}/u/login?state={quote(state)}",
            data={"username": self._username, "password": self._password, "state": state},
            timeout=15,
            allow_redirects=False,
        )
        resp = self._follow_redirects(s, resp)
        resp = self._skip_marketing_consent(s, resp)

        if resp.status_code >= 400:
            raise RuntimeError(f"Login failed (new flow): {resp.status_code} {resp.reason}")

    @staticmethod
    def _follow_redirects(s: requests.Session, resp: requests.Response) -> requests.Response:
        """Follow HTTP 3xx redirects, but only for HTTPS targets."""
        for _ in range(20):
            if resp.status_code not in (301, 302, 303, 307, 308):
                break
            location = resp.headers.get("Location", "")
            if not location:
                break
            target = urljoin(resp.url, location)
            if not target.startswith("https://"):
                break
            resp = s.get(target, timeout=15, allow_redirects=False)
        return resp

    def _skip_marketing_consent(self, s: requests.Session, resp: requests.Response) -> requests.Response:
        """Detect and skip optional VW marketing consent interstitial."""
        url = resp.url or ""
        if "/consent/marketing/" not in url:
            return resp
        qs = parse_qs(urlparse(url).query)
        callbacks = qs.get("callback", [])
        if not callbacks:
            raise RuntimeError("marketing consent page missing callback URL")
        callback = callbacks[0]
        LOG.info("Skipping marketing consent page, following callback: %s", callback)
        cb_resp = s.get(callback, timeout=15, allow_redirects=False)
        return self._follow_redirects(s, cb_resp)

    def _get(self, url: str, **kwargs) -> requests.Response:
        """GET with auto-relogin on 401/403."""
        resp = self._session.get(url, timeout=30, **kwargs)
        if resp.status_code in (401, 403):
            LOG.info("Session expired, re-authenticating")
            self.login()
            resp = self._session.get(url, timeout=30, **kwargs)
        return resp

    def vehicles(self) -> list[dict]:
        """Return list of vehicles linked to the portal account."""
        resp = self._get(
            BASE_URL + "/proxy_api/consent/me/vehicles?viewPosition=FRONT_LEFT",
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("vehicles", [])

    def identifier(self, vin: str, feed_type: str = "partial") -> str:
        """Return the data-request identifier for a VIN. feed_type is 'partial' or 'all'."""
        url = f"{BASE_URL}/proxy_api/euda-apim/datarequest/vehicles/{vin}/metadata/{feed_type}"
        resp = self._get(url, headers={"Accept": "application/json"})
        if resp.status_code == 404:
            raise RuntimeError(
                f"EU Data Act subscription not configured for {vin}. "
                "Enable it once in the browser at eu-data-act.drivesomethinggreater.com"
            )
        resp.raise_for_status()
        data = resp.json()
        ident = data.get("Identifier") or data.get("identifier", "")
        if not ident:
            raise RuntimeError(f"No data-request identifier in response for {vin}")
        return ident

    def datasets(self, vin: str, identifier: str, feed_type: str = "partial") -> list[dict]:
        """Return the list of available dataset metadata entries."""
        url = f"{BASE_URL}/proxy_api/euda-apim/datadelivery/vehicles/{vin}/{identifier}/list"
        resp = self._get(url, headers={"Accept": "application/json", "type": feed_type})
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("files", [])

    def download(self, vin: str, identifier: str, name: str, feed_type: str = "partial") -> bytes:
        """Download a single dataset ZIP archive by name."""
        url = f"{BASE_URL}/proxy_api/euda-apim/datadelivery/vehicles/{vin}/{identifier}/download"
        resp = self._get(url, headers={"filename": name, "type": feed_type})
        resp.raise_for_status()
        return resp.content


def parse_dataset(raw: bytes) -> dict[str, dict[str, Any]]:
    """
    Extract and merge data points from a dataset ZIP archive.

    Returns a dict mapping dataFieldName → {value, timestamp} where, on
    duplicate field names, the entry with the newest timestamp wins.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        json_names = [n for n in zf.namelist() if n.lower().endswith(".json")]
        if not json_names:
            raise ValueError("no JSON document in dataset ZIP")
        payload = zf.read(json_names[0])

    doc = json.loads(payload)
    result: dict[str, dict] = {}
    for point in doc.get("Data", []):
        name = point.get("dataFieldName", "")
        value = point.get("value", "")
        if not name or not value:
            continue
        ts_raw = point.get("timestampUtc")
        ts: datetime | None = None
        if ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw.rstrip("Z")).replace(tzinfo=timezone.utc)
            except ValueError:
                ts = None
        if name in result:
            existing_ts = result[name]["timestamp"]
            if existing_ts and ts and existing_ts >= ts:
                continue
        result[name] = {"value": value, "timestamp": ts}
    return result


def _parse_created_on(raw: str) -> datetime | None:
    """Parse a dataset createdOn timestamp string to a timezone-aware datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.rstrip("Z")).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
