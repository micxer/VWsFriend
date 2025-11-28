#!/usr/bin/env python3
"""
Fix weconnect authentication - replace with new VW auth flow only.
Based on CarConnectivity PR #67 and successful test_auth.py results.
Also fixes oauthlib to allow weconnect:// custom scheme specifically.
"""

# Patch 1: Fix oauthlib to allow weconnect:// scheme without disabling HTTPS enforcement
oauthlib_filepath = '/opt/venv/lib/python3.12/site-packages/oauthlib/oauth2/rfc6749/parameters.py'

try:
    with open(oauthlib_filepath, 'r') as f:
        oauthlib_content = f.read()

    # Allow weconnect:// custom scheme
    old_check = "if not is_secure_transport(uri):"
    new_check = "if not is_secure_transport(uri) and not uri.startswith('weconnect://'):"

    if old_check in oauthlib_content and "uri.startswith('weconnect://')" not in oauthlib_content:
        oauthlib_content = oauthlib_content.replace(old_check, new_check)
        with open(oauthlib_filepath, 'w') as f:
            f.write(oauthlib_content)
        print("✓ Patched oauthlib to allow weconnect:// custom scheme (HTTPS still enforced)")
    else:
        print("⚠ oauthlib already patched for custom scheme")
except Exception as e:
    print(f"⚠ Could not patch oauthlib custom scheme: {e}")

# Patch 2: Fix oauthlib state validation for VW's multi-state redirect chain
try:
    with open(oauthlib_filepath, 'r') as f:
        oauthlib_content = f.read()

    # VW uses different state values through redirect chain - skip validation for weconnect:// URIs only
    old_state_check = "if state and params.get('state', None) != state:"
    new_state_check = "if state and params.get('state', None) != state and not uri.startswith('weconnect://'):"

    if old_state_check in oauthlib_content:
        # Replace all occurrences of the state check
        count = oauthlib_content.count(old_state_check)
        oauthlib_content = oauthlib_content.replace(old_state_check, new_state_check)
        with open(oauthlib_filepath, 'w') as f:
            f.write(oauthlib_content)
        print(f"✓ Patched oauthlib state validation ({count} occurrences) for weconnect:// URIs only")
    else:
        print("⚠ oauthlib state validation already patched")
except Exception as e:
    print(f"⚠ Could not patch oauthlib state validation: {e}")

# Now patch vw_web_session.py
filepath = '/opt/venv/lib/python3.12/site-packages/weconnect/auth/vw_web_session.py'

with open(filepath, 'r') as f:
    content = f.read()

# Check if already patched
if 'Old legacy form-based flow removed' in content:
    print("✓ Already patched - new auth flow in place")
    exit(0)

print("Replacing doWebAuth with new authentication flow...")

# Complete new doWebAuth method that only uses the new flow
new_doweb_auth_method = '''    def doWebAuth(self, url: str) -> str:
        """
        Perform web authentication using the new VW identity flow.
        Old legacy form-based flow removed as it no longer works.
        """
        import re as re_module
        import logging

        LOG = logging.getLogger("weconnect")

        # Check if we already have the OAuth callback URL
        if url.startswith('weconnect://authenticated'):
            return url

        # Get authorization page, manually following redirects to avoid custom scheme issues
        max_initial_redirects = 5
        while max_initial_redirects > 0:
            # Check for custom scheme before making request
            if url.startswith('weconnect://'):
                return url

            response = self.websession.get(url, allow_redirects=False)

            if response.status_code == requests.codes['ok']:
                break

            if response.status_code in (requests.codes['found'], requests.codes['see_other']):
                if 'Location' not in response.headers:
                    raise APICompatibilityError('Forwarding without Location in headers')
                url = urljoin(url, response.headers['Location'])
                max_initial_redirects -= 1
                continue

            raise APICompatibilityError(f'Failed to fetch authorization page, status code: {response.status_code}')

        if max_initial_redirects == 0:
            raise APICompatibilityError('Too many redirects while fetching authorization page')

        # Extract all hidden form fields from the page
        login_data = {}

        # Find all input fields (including hidden ones)
        for input_match in re_module.finditer(r'<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"[^>]*>', response.text):
            field_name = input_match.group(1)
            field_value = input_match.group(2)
            login_data[field_name] = field_value

        # Verify we have the state token
        if 'state' not in login_data:
            raise APICompatibilityError('Could not find state token in authorization page')

        state = login_data['state']

        # Add/override username and password
        login_data['username'] = self.sessionuser.username
        login_data['password'] = self.sessionuser.password

        # Post credentials to new login endpoint
        login_url = f'https://identity.vwgroup.io/u/login?state={state}'

        response = self.websession.post(login_url, data=login_data, allow_redirects=False)

        if response.status_code not in (requests.codes['found'], requests.codes['see_other']):
            raise AuthentificationError(f'Login failed with status: {response.status_code}')

        if 'Location' not in response.headers:
            raise APICompatibilityError('No Location header in login response')

        # Follow redirect chain until we reach weconnect://authenticated
        # Use a plain requests.Session here instead of OAuth2Session to avoid interference
        redirect_url = response.headers['Location']

        # Create plain session with cookies from OAuth session
        plain_session = requests.Session()
        plain_session.cookies.update(self.websession.cookies)
        plain_session.headers.update(self.websession.headers)

        max_redirects = 10

        while max_redirects > 0:
            LOG.debug(f"Following redirect: {redirect_url[:80]}...")

            # Check for custom scheme
            if redirect_url.startswith('weconnect://'):
                LOG.debug(f"Got callback URL with fragment: {redirect_url[:100]}...")
                return redirect_url

            # Process non-custom scheme URLs
            redirect_url = urljoin('https://identity.vwgroup.io', redirect_url)
            response = plain_session.get(redirect_url, allow_redirects=False)

            if 'Location' not in response.headers:
                LOG.error(f"No Location header in redirect, status: {response.status_code}")
                raise APICompatibilityError('No Location header in redirect')

            redirect_url = response.headers['Location']
            max_redirects -= 1

        raise APICompatibilityError('Too many redirects during authentication')

'''

# Replace the entire doWebAuth method
doweb_start = content.find('    def doWebAuth(self, url: str) -> str:')
if doweb_start != -1:
    # Find the end of the method (next method definition at same indentation)
    next_method = content.find('\n    def ', doweb_start + 1)
    if next_method != -1:
        content = content[:doweb_start] + new_doweb_auth_method + content[next_method:]
        print("✓ Replaced entire doWebAuth method with new auth flow")
    else:
        print("✗ Could not find end of doWebAuth method")
        exit(1)
else:
    print("✗ doWebAuth method not found")
    exit(1)

# Write the patched file
with open(filepath, 'w') as f:
    f.write(content)

print(f"✓ Successfully patched {filepath}")
print("✓ New authentication flow active (legacy flow removed)")

# Patch 3: Fix openid_session.py to use implicit flow parser
openid_filepath = '/opt/venv/lib/python3.12/site-packages/weconnect/auth/openid_session.py'

try:
    with open(openid_filepath, 'r') as f:
        openid_content = f.read()

    # Check if already patched
    if 'parse_implicit_response' in openid_content and 'parseFromFragment' in openid_content:
        print("✓ openid_session.py already patched for implicit flow")
    else:
        # Add parse_implicit_response to imports
        # flake8: noqa: E501
        old_import = "from oauthlib.oauth2.rfc6749.parameters import parse_authorization_code_response, parse_token_response, prepare_grant_uri"
        # flake8: noqa: E501
        new_import = "from oauthlib.oauth2.rfc6749.parameters import parse_authorization_code_response, parse_implicit_response, parse_token_response, prepare_grant_uri"

        if old_import in openid_content:
            openid_content = openid_content.replace(old_import, new_import)
            print("✓ Added parse_implicit_response to imports")

        # Fix parseFromFragment to use implicit flow parser
        old_parse = "        self.token = parse_authorization_code_response(authorization_response, state=state)"
        new_parse = "        self.token = parse_implicit_response(authorization_response, state=state)"

        if old_parse in openid_content:
            openid_content = openid_content.replace(old_parse, new_parse)

            with open(openid_filepath, 'w') as f:
                f.write(openid_content)

            print("✓ Patched parseFromFragment to use implicit flow parser")
        else:
            print("⚠ Could not find parseFromFragment parse call to patch")
except Exception as e:
    print(f"⚠ Could not patch openid_session.py: {e}")
