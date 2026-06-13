"""
EU Data Act polling connector for VWsFriend.

Polls the eu-data-act.drivesomethinggreater.com portal every ~15 minutes
and writes data into the same database models used by the WeConnect-based
agents (Battery, Range, Charge, Vehicle).

Fields available from the portal:
  battery_state_report.soc / batteryStatus.currentSOC_pct — SOC %
  cruising_range_primary_engine / batteryStatus.cruisingRange.range — electric range km
  charging_state_report.current_charge_state — charging state
  battery_state_report.charge_power — charge power kW
  battery_state_report.remaining_charging_time_complete — remaining time (seconds)
  remaining_charging_time — remaining time (minutes, official dict name)
  batteryStatus.hvBatteryTemperature.temperatureValue / hvbatterytemperature_info.* — HV battery temp
  batteryStatus.hvBatteryTemperature.temperatureUnit — unit (CELSIUS or KELVIN)
  mileage_info.value — odometer km
  settings.target_soc / targetSoc_pct — target SOC %

Not available: GPS position → Trip/RefuelSession agents remain dormant.
Climatization, warning lights, maintenance: no data in EU Data Act feed.
"""

import logging
import time
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError

from weconnect.elements.charging_status import ChargingStatus
from weconnect.elements.climatization_status import ClimatizationStatus

from vwsfriend.model.battery import Battery
from vwsfriend.model.battery_temperature import BatteryTemperature
from vwsfriend.model.charge import Charge
from vwsfriend.model.climatization import Climatization
from vwsfriend.model.maintenance import Maintenance, MaintenanceType
from vwsfriend.model.range import Range
from vwsfriend.model.vehicle import Vehicle
from vwsfriend.agents.eu_data_act.api import (
    EUDataActAPI,
    parse_dataset,
    _parse_created_on,
)

LOG = logging.getLogger("VWsFriend")

# EU Data Act field names.
# "partial" feed uses the flat evcc-style names (from evcc types.go).
# "all" feed uses richer dotted names as seen in the bulk export.
# We try all candidates in order; first non-empty wins.
_F_SOC = (
    "battery_state_report.soc", "state_of_charge", "hv_soc", "battery_level_HV.value",
    "batteryStatus.currentSOC_pct",
    "ChargingEvent.[*].BatteryStatus.[*].currentSocPct",
    "battery_charging_status",
    "secondaryEngineBatteryStateOfChargeIndication",
    "RBC.vehicleStates.[*].soc",
)
_F_RANGE_ELECTRIC = (
    "cruising_range_primary_engine",
    "batteryStatus.cruisingRange.range",
    "ChargingEvent.[*].BatteryStatus.[*].cruisingRangeElectricKm",
    "cruise_range_primary_info.value",
    "estimatedcruisingrangeprimary.value",
    "battery_state_report.cruising_ranges.[*].range",
)
_F_RANGE_SECONDARY = ("cruising_range_secondary_engine",)
_F_RANGE_COMBINED = (
    "cruising_range_combined",
    "cruising_range_combine",
)
_F_ODOMETER = (
    "mileage", "mileage.value", "mileage_info.value",
    "totalDistance", "DW_Kilometerstand", "mileage_km",
    "short_term_data_mileage",
)
_F_CHARGING_STATE = (
    "charging_state",
    "charging_state_report.current_charge_state",
    "chargingStatus.currentChargeState",
    "ChargingEvent.[*].ChargingStatus.[*].chargingState",
    "07_wallbox_elli charging_state",
    "09_11_csm charging_state",
)
_F_CHARGE_POWER = (
    "battery_state_report.charge_power",
    "chargingStatus.chargePower_kW",
    "ChargingEvent.[*].ChargingStatus.[*].chargePowerKW",
    "actual_charge_power",
)
_F_CHARGE_RATE = (
    "battery_state_report.charge_rate",
    "ChargingEvent.[*].ChargingStatus.[*].chargeRateKmph",
    "actual_charge_rate",
)
_F_PLUG_CONNECTION = (
    "plug_state",
    "plug_connection_state",
    "plugStatusItem.plugConnectionState",
    "ChargingEvent.[*].PlugStatus.[*].plugConnectionState",
    "charging_plug1_connectionstate",
    "charging_plug2_connectionstate",
)
_F_TARGET_SOC = (
    "settings.target_soc",
    "targetSoc_pct",
    "profiles.targetSOCPercentage",
    "ChargingProfileStatus.[*].ChargingProfile.[*].targetSOCPct",
    "active_target_soc",
    "profile_state_report.profiles.[*].target_soc",
)
_F_REMAINING_TIME = (
    "remaining_charging_time",
    "chargingStatus.remainingChargingTimeToCompleteInMin",
    "ChargingEvent.[*].ChargingStatus.[*].remainingChargingTimeToCompleteMin",
)
_F_BATT_TEMP_MAX = (
    "batteryStatus.hvBatteryTemperature.temperatureValue",
    "BatteryTemperature.[*].temperatureHvBatteryMaxKelvin",
    "hvbatterytemperature_info.max_temperature.value",
    "max_temperature",
)
_F_BATT_TEMP_MIN = (
    "BatteryTemperature.[*].temperatureHvBatteryMinKelvin",
    "hvbatterytemperature_info.min_temperature.value",
    "min_temperature",
)
_F_BATT_TEMP_UNIT = (
    "batteryStatus.hvBatteryTemperature.temperatureUnit",
    "hvbatterytemperature_info.max_temperature.unit",
    "hvbatterytemperature_info.min_temperature.unit",
)

# Maintenance — generic time/distance fallbacks (non-indexed)
_F_MAINTENANCE_DAYS_FALLBACK = (
    "ServiceInterval.[*].DueInTime",
    "service_maintenance_info.due_in_time.value",
    "Maintenance.[*].inspectionDueDays",
)
_F_MAINTENANCE_KM_FALLBACK = (
    "ServiceInterval.[*].DueInDistance",
    "service_maintenance_info.due_in_distance.value",
    "Maintenance.[*].inspectionDueKm",
)
_F_MAINTENANCE_OIL_DAYS_FALLBACK = ("oilServiceDue_days",)
_F_MAINTENANCE_OIL_KM_FALLBACK = ("oilServiceDue_km",)

# Climatisation fields
_F_CLIMA_STATE = (
    "envelope.[*].report.status",
)
_F_CLIMA_REMAINING = (
    "remaining_climatisation_time",
    "remaining_climate_time",
    "climatisationStatusWrapper.remainingClimatisationTimeInMin",
    "envelope.[*].report.remainingClimatizationTime_min.seconds",
)

# Parking GPS
_F_PARKING_LAT = (
    "ParkingPosition.[*].lat",
    "parking_position.latitude",
)
_F_PARKING_LON = (
    "ParkingPosition.[*].lon",
    "parking_position.longitude",
)

# Map portal charging_state strings to WeConnect ChargingStatus.ChargingState enum
_CHARGING_STATE_MAP: dict[str, ChargingStatus.ChargingState] = {
    "charging": ChargingStatus.ChargingState.CHARGING,
    "chargestatecharginghvbattery": ChargingStatus.ChargingState.CHARGING,
    "notreadyforcharging": ChargingStatus.ChargingState.NOT_READY_FOR_CHARGING,
    "chargestatenotreadyforcharging": ChargingStatus.ChargingState.NOT_READY_FOR_CHARGING,
    "readyforcharging": ChargingStatus.ChargingState.READY_FOR_CHARGING,
    "chargestatereadyforcharging": ChargingStatus.ChargingState.READY_FOR_CHARGING,
    "off": ChargingStatus.ChargingState.OFF,
    "chargestateoff": ChargingStatus.ChargingState.OFF,
    "error": ChargingStatus.ChargingState.ERROR,
    "chargestateerror": ChargingStatus.ChargingState.ERROR,
    "conservationcharging": ChargingStatus.ChargingState.CONSERVATION,
    "chargestateconservationcharging": ChargingStatus.ChargingState.CONSERVATION,
    "chargepurposereachedandnotconservationcharging":
        ChargingStatus.ChargingState.CHARGE_PURPOSE_REACHED_NOT_CONSERVATION_CHARGING,
    "chargepurposereachedandconservationcharging":
        ChargingStatus.ChargingState.CHARGE_PURPOSE_REACHED_CONSERVATION,
    "discharging": ChargingStatus.ChargingState.DISCHARGING,
    "chargestatedischarging": ChargingStatus.ChargingState.DISCHARGING,
}

# How many of the newest datasets to backfill on first poll
_MAX_BACKFILL = 8

# Minimum interval between polls even if new data is available
_MIN_POLL_INTERVAL = 60  # seconds


def _get_point(data: dict[str, dict], *field_names: str) -> dict | None:
    """Return the first non-empty point from data for the given field name candidates.

    Field names containing '[*]' match any array index, e.g.
    'service_maintenances.[*].due_in_time.current_value' matches
    'service_maintenances.[0].due_in_time.current_value' and
    'service_maintenances.[1].due_in_time.current_value' etc.
    The match with the lowest index wins.
    """
    for name in field_names:
        if "[*]" in name:
            prefix, _, suffix = name.partition("[*]")
            for k, v in data.items():
                if k.startswith(prefix) and k.endswith(suffix) and v and v.get("value"):
                    return v
        else:
            p = data.get(name)
            if p and p.get("value"):
                return p
    return None


def _float(data: dict[str, dict], *field_names: str) -> float | None:
    p = _get_point(data, *field_names)
    if p is None:
        return None
    try:
        raw = str(p["value"])
        # Strip trailing unit suffixes e.g. "89.0km/h", "10.5kW"
        val = raw.rstrip("mhzAkKwW/ ")
        return float(val)
    except (ValueError, TypeError):
        return None


def _seconds_to_minutes(data: dict[str, dict], *field_names: str) -> int | None:
    """Parse a value that may be in seconds (e.g. '1200s') and return minutes."""
    p = _get_point(data, *field_names)
    if p is None:
        return None
    try:
        raw = str(p["value"])
        if raw.endswith("s") and not raw.endswith("ms"):
            return int(round(float(raw[:-1]) / 60))
        # Already in minutes (no unit or explicit min)
        return int(round(float(raw.rstrip("min ").rstrip())))
    except (ValueError, TypeError):
        return None


def _int(data: dict[str, dict], *field_names: str) -> int | None:
    v = _float(data, *field_names)
    return int(round(v)) if v is not None else None


def _ts(data: dict[str, dict], *field_names: str) -> datetime | None:
    p = _get_point(data, *field_names)
    return p["timestamp"] if p else None


def _newest_ts(data: dict[str, dict]) -> datetime | None:
    """Return the most recent timestamp across all data points.

    car_captured_time carries the actual capture time for partial-feed datasets
    where all individual field timestamps are None. Always consider it as a
    candidate so partial data is stored with today's time, not the stale
    timestamps from the all-feed seed.
    """
    ts = None
    for p in data.values():
        pt = p.get("timestamp")
        if pt and (ts is None or pt > ts):
            ts = pt
    # Always consider car_captured_time — it may be newer than any field timestamp
    p = data.get("car_captured_time")
    if p and p.get("value"):
        try:
            cct = datetime.fromisoformat(p["value"].rstrip("Z")).replace(tzinfo=timezone.utc)
            if ts is None or cct > ts:
                ts = cct
        except ValueError:
            pass
    return ts


def _map_charging_state(value: str) -> ChargingStatus.ChargingState | None:
    if not value:
        return None
    key = value.lower().replace("_", "").replace("-", "").replace(" ", "")
    return _CHARGING_STATE_MAP.get(key)


class EUDataActVehicleState:
    """Per-vehicle polling state."""

    def __init__(self, vin: str) -> None:
        self.vin = vin
        self.identifier: str = ""        # partial feed identifier
        self.all_identifier: str = ""    # all-data feed identifier
        self.after: datetime | None = None  # high-water mark for partial datasets
        self.all_seeded: bool = False    # whether all-data seed was done
        self.merged: dict[str, dict] = {}  # merged data points (newest-wins)
        self.last_poll: float = 0.0


class EUDataActConnector:
    """
    Polls the EU Data Act portal for all configured VINs and writes
    Battery, Range, and Charge snapshots to the database.

    Instantiate once and call update() from the main loop.
    """

    def __init__(self, session_factory, username: str, password: str, brand: str = "volkswagen") -> None:
        self._session_factory = session_factory
        self._api = EUDataActAPI(username=username, password=password, brand=brand)
        self._vehicles: dict[str, EUDataActVehicleState] = {}
        self._logged_in = False

    def _ensure_logged_in(self) -> None:
        if not self._logged_in:
            LOG.info("EU Data Act: logging in as %s", self._api._username)
            self._api.login()
            self._logged_in = True

    def _discover_vins(self) -> list[str]:
        """Fetch VINs from portal and return them."""
        try:
            vehicles = self._api.vehicles()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.error("EU Data Act: failed to fetch vehicle list: %s", exc)
            return []
        vins = []
        for v in vehicles:
            vin = v.get("vin") or v.get("vehicleIdentificationNumber", "")
            if vin:
                vins.append(vin)
                LOG.info("EU Data Act: found vehicle %s (%s)", vin,
                         v.get("nickName") or v.get("vehicleNickname") or v.get("nickname") or v.get("modelName", ""))
        return vins

    def update(self) -> None:  # noqa: C901
        """
        Fetch new datasets for all known VINs and persist the data.
        Safe to call from the main loop at any interval — internally
        throttled to _MIN_POLL_INTERVAL per vehicle.
        """
        try:
            self._ensure_logged_in()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.error("EU Data Act: login failed: %s", exc)
            self._logged_in = False
            return

        # Discover VINs on first call
        if not self._vehicles:
            for vin in self._discover_vins():
                self._vehicles[vin] = EUDataActVehicleState(vin)

        now = time.monotonic()
        for vin, state in self._vehicles.items():
            if now - state.last_poll < _MIN_POLL_INTERVAL:
                continue
            try:
                self._update_vehicle(state)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                LOG.error("EU Data Act: error updating vehicle %s: %s", vin, exc)
            state.last_poll = now

    def _update_vehicle(self, state: EUDataActVehicleState) -> None:  # noqa: C901
        """Download new datasets for one vehicle and merge into state.merged."""
        # Seed from 'all' feed once to get rich historical data + battery temp
        if not state.all_seeded:
            self._seed_from_all(state)

        # Resolve partial identifier once
        if not state.identifier:
            try:
                state.identifier = self._api.identifier(state.vin, feed_type="partial")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                LOG.error("EU Data Act: cannot get identifier for %s: %s", state.vin, exc)
                return

        raw_datasets = self._api.datasets(state.vin, state.identifier, feed_type="partial")

        # Parse createdOn and filter out "no content found" placeholders
        datasets: list[tuple[datetime, str]] = []
        for entry in raw_datasets:
            name = entry.get("name", "")
            if name.lower().endswith("_no_content_found.zip"):
                continue
            created_raw = entry.get("createdOn", "")
            created = _parse_created_on(created_raw) if isinstance(created_raw, str) else None
            if created is None and isinstance(created_raw, dict):
                created = _parse_created_on(created_raw.get("$date", ""))
            if created is None:
                LOG.warning("EU Data Act: unparseable createdOn for dataset %s: %r", name, created_raw)
                continue
            datasets.append((created, name))

        datasets.sort(key=lambda x: x[0])  # oldest first

        # On first poll backfill newest _MAX_BACKFILL datasets; after that only new ones
        if state.after is None:
            pending = datasets[-_MAX_BACKFILL:]
        else:
            pending = [(ts, name) for ts, name in datasets if ts > state.after]

        if not pending:
            LOG.debug("EU Data Act: no new partial datasets for %s", state.vin)
            if not state.merged:
                return  # nothing at all yet
            self._persist(state)
            return

        for created, name in pending:
            try:
                raw = self._api.download(state.vin, state.identifier, name, feed_type="partial")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                LOG.error("EU Data Act: download failed for %s/%s: %s", state.vin, name, exc)
                continue

            try:
                data = parse_dataset(raw)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                LOG.error("EU Data Act: parse failed for %s/%s: %s", state.vin, name, exc)
                continue

            # Merge: newest-timestamp-wins per field; when both timestamps are
            # None (partial feed), last-downloaded dataset wins.
            for field, point in data.items():
                existing = state.merged.get(field)
                if existing is None:
                    state.merged[field] = point
                    continue
                existing_ts = existing.get("timestamp")
                new_ts = point.get("timestamp")
                if new_ts and existing_ts and new_ts > existing_ts:
                    state.merged[field] = point
                elif new_ts and not existing_ts:
                    state.merged[field] = point
                elif not new_ts and not existing_ts:
                    state.merged[field] = point  # last-write-wins for timestamp-less fields

            if state.after is None or created > state.after:
                state.after = created

            LOG.debug("EU Data Act: merged dataset %s for %s (created %s)", name, state.vin,
                      created.isoformat())

        self._persist(state)

    def _seed_from_all(self, state: EUDataActVehicleState) -> None:  # noqa: C901
        """Download the 'all' bulk dataset once to seed rich historical data."""
        state.all_seeded = True  # mark done even on error so we don't retry forever
        try:
            if not state.all_identifier:
                state.all_identifier = self._api.identifier(state.vin, feed_type="all")
        except Exception as exc:  # pylint: disable=broad-exception-caught  # pylint: disable=broad-exception-caught  # noqa: BLE001
            LOG.info("EU Data Act: no 'all' feed for %s (%s), using partial only", state.vin, exc)
            return
        try:
            raw_datasets = self._api.datasets(state.vin, state.all_identifier, feed_type="all")
        except Exception as exc:  # pylint: disable=broad-exception-caught  # pylint: disable=broad-exception-caught  # noqa: BLE001
            LOG.warning("EU Data Act: failed to list 'all' datasets for %s: %s", state.vin, exc)
            return
        if not raw_datasets:
            return
        # Download the newest content dataset
        content = [d for d in raw_datasets if not d.get("name", "").lower().endswith("_no_content_found.zip")]
        if not content:
            return
        newest = max(content, key=lambda d: d.get("createdOn", ""))
        try:
            raw = self._api.download(state.vin, state.all_identifier, newest["name"], feed_type="all")
            data = parse_dataset(raw)
        except Exception as exc:  # pylint: disable=broad-exception-caught  # pylint: disable=broad-exception-caught  # noqa: BLE001
            LOG.warning("EU Data Act: failed to download/parse 'all' dataset for %s: %s", state.vin, exc)
            return
        # Merge as seed (partial feed data will override with newer timestamps later)
        for field, point in data.items():
            if field not in state.merged:
                state.merged[field] = point
        LOG.info("EU Data Act: seeded %d fields from 'all' dataset for %s (%s)",
                 len(data), state.vin, newest.get("createdOn", ""))

    def _persist(self, state: EUDataActVehicleState) -> None:
        """Write Battery, Range, and Charge records to DB from merged data."""
        data = state.merged
        if not data:
            return

        session = self._session_factory()
        try:
            db_vehicle = session.query(Vehicle).filter(Vehicle.vin == state.vin).first()
            if db_vehicle is None:
                db_vehicle = Vehicle(state.vin)
                session.add(db_vehicle)
                session.commit()

            self._persist_battery(session, db_vehicle, data)
            self._persist_battery_temperature(session, db_vehicle, data)
            self._persist_range(session, db_vehicle, data)
            self._persist_charge(session, db_vehicle, data)
            self._persist_climatisation(session, db_vehicle, data)
            self._persist_maintenance(session, db_vehicle, data)
            session.commit()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.error("EU Data Act: DB persist error for %s: %s", state.vin, exc)
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def _persist_battery(session, vehicle: Vehicle, data: dict[str, dict]) -> None:
        soc = _int(data, *_F_SOC)
        range_km = _int(data, *_F_RANGE_ELECTRIC, *_F_RANGE_COMBINED)
        if soc is None and range_km is None:
            return

        ts = _newest_ts(data) or _ts(data, *_F_SOC) or _ts(data, *_F_RANGE_ELECTRIC)
        if ts is None:
            LOG.debug("EU Data Act: no timestamp for battery data of %s, skipping", vehicle.vin)
            return

        last = (session.query(Battery)
                .filter(Battery.vehicle_vin == vehicle.vin, Battery.carCapturedTimestamp.isnot(None))
                .order_by(Battery.carCapturedTimestamp.desc())
                .first())

        if last is not None and last.carCapturedTimestamp == ts:
            return
        if last is not None and last.currentSOC_pct == soc and last.cruisingRangeElectric_km == range_km:
            return

        entry = Battery(vehicle, ts, soc, range_km)
        try:
            session.add(entry)
            session.flush()
            LOG.info("EU Data Act: battery SOC=%s%% range=%s km for %s at %s",
                     soc, range_km, vehicle.vin, ts.isoformat())
        except IntegrityError:
            session.rollback()

    @staticmethod
    def _persist_battery_temperature(session, vehicle: Vehicle, data: dict[str, dict]) -> None:
        # Determine unit: Kelvin or Celsius
        unit_p = _get_point(data, *_F_BATT_TEMP_UNIT)
        in_celsius = unit_p and unit_p.get("value", "").upper() == "CELSIUS"
        max_val = _float(data, *_F_BATT_TEMP_MAX)
        min_val = _float(data, *_F_BATT_TEMP_MIN)
        if max_val is None and min_val is None:
            return
        # Convert Celsius to Kelvin if needed
        if in_celsius:
            if max_val is not None:
                max_val = max_val + 273.15
            if min_val is not None:
                min_val = min_val + 273.15
        ts = _newest_ts(data) or _ts(data, *_F_BATT_TEMP_MAX, *_F_BATT_TEMP_MIN)
        if ts is None:
            return
        last = (session.query(BatteryTemperature)
                .filter(BatteryTemperature.vehicle_vin == vehicle.vin,
                        BatteryTemperature.carCapturedTimestamp.isnot(None))
                .order_by(BatteryTemperature.carCapturedTimestamp.desc())
                .first())
        if last is not None and last.carCapturedTimestamp == ts:
            return
        if last is not None and last.temperatureHvBatteryMax_K == max_val and last.temperatureHvBatteryMin_K == min_val:
            return
        entry = BatteryTemperature(vehicle, ts, min_val, max_val)
        try:
            session.add(entry)
            session.flush()
            LOG.debug("EU Data Act: battery temp min=%.1fK max=%.1fK for %s at %s",
                      min_val or 0, max_val or 0, vehicle.vin, ts.isoformat())
        except IntegrityError:
            session.rollback()

    @staticmethod
    def _persist_range(session, vehicle: Vehicle, data: dict[str, dict]) -> None:
        primary_soc = _int(data, *_F_SOC)
        primary_range = _int(data, *_F_RANGE_ELECTRIC)
        secondary_range = _int(data, *_F_RANGE_SECONDARY)
        total_range = _int(data, *_F_RANGE_COMBINED) or (primary_range if secondary_range is None else None)
        secondary_soc = None  # not available in EU Data Act

        if primary_soc is None and primary_range is None and secondary_range is None:
            return

        ts = _newest_ts(data) or _ts(data, *_F_RANGE_ELECTRIC, *_F_RANGE_COMBINED, *_F_SOC)
        if ts is None:
            return

        last = (session.query(Range)
                .filter(Range.vehicle_vin == vehicle.vin, Range.carCapturedTimestamp.isnot(None))
                .order_by(Range.carCapturedTimestamp.desc())
                .first())

        if last is not None and last.carCapturedTimestamp == ts:
            return
        if last is not None and (last.totalRange_km == total_range
                                 and last.primary_currentSOC_pct == primary_soc
                                 and last.primary_remainingRange_km == primary_range
                                 and last.secondary_remainingRange_km == secondary_range):
            return

        entry = Range(vehicle, ts, total_range, primary_soc, primary_range, secondary_soc, secondary_range)
        try:
            session.add(entry)
            session.flush()
            LOG.debug("EU Data Act: range total=%s primary=%s secondary=%s for %s at %s",
                      total_range, primary_range, secondary_range, vehicle.vin, ts.isoformat())
        except IntegrityError:
            session.rollback()

    @staticmethod
    def _persist_charge(session, vehicle: Vehicle, data: dict[str, dict]) -> None:  # noqa: C901
        charging_state_val = None
        charging_ts = None
        for field in _F_CHARGING_STATE:
            p = data.get(field)
            if p and p.get("value"):
                charging_state_val = p["value"]
                charging_ts = p.get("timestamp")
                break

        if charging_state_val is None:
            return

        charging_state = _map_charging_state(charging_state_val)
        if charging_state is None:
            LOG.warning("EU Data Act: unknown charging_state value '%s' for %s", charging_state_val, vehicle.vin)

        # battery_state_report.remaining_charging_time_complete is in seconds ("1200s")
        # remaining_charging_time (continuous dict) is in minutes (int)
        remaining_min = _seconds_to_minutes(data, "battery_state_report.remaining_charging_time_complete")
        if remaining_min is None:
            remaining_min = _int(data, "remaining_charging_time",
                                 "ChargingEvent.[*].ChargingStatus.[*].remainingChargingTimeToCompleteMin")
        # 65535 is the portal's "unavailable" sentinel (evcc convention)
        if remaining_min == 65535:
            remaining_min = None

        charge_power_kw = _float(data, *_F_CHARGE_POWER)
        charge_rate_kmph = _float(data, *_F_CHARGE_RATE)

        ts = charging_ts if charging_ts else _newest_ts(data)
        if ts is None:
            return

        last = (session.query(Charge)
                .filter(Charge.vehicle_vin == vehicle.vin, Charge.carCapturedTimestamp.isnot(None))
                .order_by(Charge.carCapturedTimestamp.desc())
                .first())

        if last is not None and last.carCapturedTimestamp == ts:
            return
        if last is not None and (last.chargingState == charging_state
                                 and last.remainingChargingTimeToComplete_min == remaining_min
                                 and last.chargePower_kW == charge_power_kw):
            return

        entry = Charge(
            vehicle=vehicle,
            carCapturedTimestamp=ts,
            remainingChargingTimeToComplete_min=remaining_min,
            chargingState=charging_state,
            chargeMode=None,
            chargePower_kW=charge_power_kw,
            chargeRate_kmph=charge_rate_kmph,
        )
        try:
            session.add(entry)
            session.flush()
            LOG.debug("EU Data Act: charge state=%s remaining=%s min for %s at %s",
                      charging_state_val, remaining_min, vehicle.vin, ts.isoformat())
        except IntegrityError:
            session.rollback()

    @staticmethod
    def _persist_climatisation(session, vehicle: Vehicle, data: dict[str, dict]) -> None:  # noqa: C901
        state_val = _get_point(data, *_F_CLIMA_STATE)
        if state_val is None:
            return

        raw = state_val.get("value", "").upper()
        state_map = {
            "OFF": ClimatizationStatus.ClimatizationState.OFF,
            "HEATING": ClimatizationStatus.ClimatizationState.HEATING,
            "COOLING": ClimatizationStatus.ClimatizationState.COOLING,
            "VENTILATION": ClimatizationStatus.ClimatizationState.VENTILATION,
        }
        clima_state = state_map.get(raw)
        if clima_state is None:
            return

        remaining_p = _get_point(data, *_F_CLIMA_REMAINING)
        remaining_min = None
        if remaining_p:
            try:
                val = str(remaining_p["value"])
                if val.endswith("s"):
                    remaining_min = int(round(float(val[:-1]) / 60))
                else:
                    remaining_min = int(round(float(val)))
            except (ValueError, TypeError):
                pass

        ts = _newest_ts(data)
        if ts is None:
            return

        last = (session.query(Climatization)
                .filter(Climatization.vehicle_vin == vehicle.vin,
                        Climatization.carCapturedTimestamp.isnot(None))
                .order_by(Climatization.carCapturedTimestamp.desc())
                .first())

        if last is not None and last.carCapturedTimestamp == ts:
            return
        if last is not None and (last.climatisationState == clima_state
                                 and last.remainingClimatisationTime_min == remaining_min):
            return

        entry = Climatization(vehicle, ts, remaining_min, clima_state)
        try:
            session.add(entry)
            session.flush()
            LOG.debug("EU Data Act: climatisation state=%s remaining=%s min for %s at %s",
                      raw, remaining_min, vehicle.vin, ts.isoformat())
        except IntegrityError:
            session.rollback()

    @staticmethod
    def _maintenance_entries(data: dict[str, dict]) -> list[tuple]:  # noqa: C901
        """Return list of (MaintenanceType, due_days, due_km) from data."""
        entries: list[tuple] = []
        sm_indices: set[str] = set()
        for k in data:
            if k.startswith("service_maintenances.[") and "]." in k:
                sm_indices.add(k.split("]")[0].split("[")[1])
        for idx in sorted(sm_indices):
            prefix = f"service_maintenances.[{idx}]."
            stype_p = data.get(f"{prefix}service_type")
            stype_val = (stype_p.get("value", "") if stype_p else "").upper()
            if "INSPECTION" in stype_val or stype_val == "0":
                mtype = MaintenanceType.INSPECTION
            elif "OIL" in stype_val or stype_val == "1":
                mtype = MaintenanceType.OIL_SERVICE
            else:
                continue
            try:
                p = data.get(f"{prefix}due_in_time.current_value")
                due_days = int(round(float(p["value"]))) if p and p.get("value") else None
            except (ValueError, TypeError):
                due_days = None
            try:
                p = data.get(f"{prefix}due_in_distance.current_value")
                due_km = int(round(float(p["value"]))) if p and p.get("value") else None
            except (ValueError, TypeError):
                due_km = None
            if due_days is not None or due_km is not None:
                entries.append((mtype, due_days, due_km))
        if not entries:
            insp_days, insp_km = _int(data, *_F_MAINTENANCE_DAYS_FALLBACK), _int(data, *_F_MAINTENANCE_KM_FALLBACK)
            if insp_days is not None or insp_km is not None:
                entries.append((MaintenanceType.INSPECTION, insp_days, insp_km))
            oil_days, oil_km = _int(data, *_F_MAINTENANCE_OIL_DAYS_FALLBACK), _int(data, *_F_MAINTENANCE_OIL_KM_FALLBACK)
            if oil_days is not None or oil_km is not None:
                entries.append((MaintenanceType.OIL_SERVICE, oil_days, oil_km))
        return entries

    @staticmethod
    def _persist_maintenance(session, vehicle: Vehicle, data: dict[str, dict]) -> None:
        ts = _newest_ts(data)
        odometer = _int(data, *_F_ODOMETER)
        for mtype, due_days, due_km in EUDataActConnector._maintenance_entries(data):
            last = (session.query(Maintenance)
                    .filter(Maintenance.vehicle_vin == vehicle.vin,
                            Maintenance.type == mtype)
                    .order_by(Maintenance.id.desc())
                    .first())

            if last is not None and last.due_in_days == due_days and last.due_in_km == due_km:
                continue

            entry = Maintenance(vehicle, ts, odometer, mtype, due_days, due_km)
            try:
                session.add(entry)
                session.flush()
                LOG.info("EU Data Act: maintenance %s due_days=%s due_km=%s for %s",
                         mtype.name, due_days, due_km, vehicle.vin)
            except IntegrityError:
                session.rollback()

    def commit(self) -> None:
        pass  # updates are committed inline in _persist()
