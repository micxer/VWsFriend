"""Tests for ChargeAgent adaptive observer registration."""
import unittest
from unittest.mock import Mock, MagicMock, patch, call
from sqlalchemy.orm import Session

from vwsfriend.agents.charge_agent import ChargeAgent
from vwsfriend.model.vehicle import Vehicle
from vwsfriend.privacy import Privacy
from weconnect.addressable import AddressableLeaf


class TestChargeAgentAdaptiveInitialization(unittest.TestCase):
    """Test ChargeAgent's ability to register observers adaptively when data becomes available."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_session = Mock(spec=Session)
        self.mock_vehicle = Mock(spec=Vehicle)
        self.mock_vehicle.vin = "WVWZZZE1ZMP1234567"

        # Mock session.merge to return the vehicle
        self.mock_session.merge.return_value = self.mock_vehicle

        # Mock session.query to return empty results (no existing charges/sessions)
        mock_query = Mock()
        mock_query.filter.return_value.order_by.return_value.first.return_value = None
        self.mock_session.query.return_value = mock_query

    def test_immediate_initialization_with_charging_status_available(self):
        """Test that observers are registered immediately when charging status is available at init."""
        # Setup: charging status is available and enabled
        mock_weconnect_vehicle = self._create_mock_vehicle_with_charging_status(enabled=True)
        self.mock_vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent
        agent = ChargeAgent(self.mock_session, self.mock_vehicle, [])

        # Assert: Observers should be registered immediately
        self.assertTrue(agent.chargingStatusObserversRegistered)
        self.assertTrue(agent.plugStatusObserversRegistered)

        # Verify that observers were added
        charging_status = mock_weconnect_vehicle.domains['charging']['chargingStatus']
        charging_status.carCapturedTimestamp.addObserver.assert_called()
        charging_status.chargingState.addObserver.assert_called()
        charging_status.chargePower_kW.addObserver.assert_called()

        plug_status = mock_weconnect_vehicle.domains['charging']['plugStatus']
        plug_status.plugConnectionState.addObserver.assert_called()
        plug_status.plugLockState.addObserver.assert_called()

        # Verify that deferred registration observer was NOT added
        mock_weconnect_vehicle.addObserver.assert_not_called()

    def test_deferred_initialization_when_charging_status_not_available(self):
        """Test that deferred observer is registered when charging status is not available at init."""
        # Setup: charging status is NOT enabled initially
        mock_weconnect_vehicle = self._create_mock_vehicle_with_charging_status(enabled=False)
        self.mock_vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent
        agent = ChargeAgent(self.mock_session, self.mock_vehicle, [])

        # Assert: Observers should NOT be registered yet
        self.assertFalse(agent.chargingStatusObserversRegistered)
        self.assertFalse(agent.plugStatusObserversRegistered)

        # Verify that deferred registration observer WAS added
        mock_weconnect_vehicle.addObserver.assert_called_once()
        call_args = mock_weconnect_vehicle.addObserver.call_args
        self.assertEqual(call_args[0][1], AddressableLeaf.ObserverEvent.UPDATED_FROM_SERVER)

    def test_deferred_observer_registers_when_status_becomes_available(self):
        """Test that observers are registered when charging status becomes available later."""
        # Setup: charging status NOT available initially
        mock_weconnect_vehicle = self._create_mock_vehicle_with_charging_status(enabled=False)
        self.mock_vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act 1: Initialize ChargeAgent (status not available)
        agent = ChargeAgent(self.mock_session, self.mock_vehicle, [])

        # Assert 1: Deferred observer should be registered
        self.assertFalse(agent.chargingStatusObserversRegistered)
        self.assertFalse(agent.plugStatusObserversRegistered)
        mock_weconnect_vehicle.addObserver.assert_called_once()

        # Capture the deferred callback
        deferred_callback = mock_weconnect_vehicle.addObserver.call_args[0][0]

        # Act 2: Simulate charging status becoming available
        self._enable_charging_status(mock_weconnect_vehicle)
        deferred_callback(None, None)  # Trigger the deferred observer

        # Assert 2: Observers should now be registered
        self.assertTrue(agent.chargingStatusObserversRegistered)
        self.assertTrue(agent.plugStatusObserversRegistered)

        # Verify that observers were added
        charging_status = mock_weconnect_vehicle.domains['charging']['chargingStatus']
        charging_status.carCapturedTimestamp.addObserver.assert_called()

        # Verify that deferred observer was removed
        mock_weconnect_vehicle.removeObserver.assert_called_once()

    def test_partial_availability_registers_available_observers(self):
        """Test that available observers are registered even if others are not ready."""
        # Setup: charging status available, but plug status not available
        mock_weconnect_vehicle = Mock()
        mock_weconnect_vehicle.statusExists.side_effect = lambda domain, status: (
            status == 'chargingStatus'  # Only chargingStatus exists
        )

        # Create charging status (enabled)
        charging_status = self._create_mock_charging_status(enabled=True)
        mock_weconnect_vehicle.domains = {
            'charging': {
                'chargingStatus': charging_status,
                'plugStatus': Mock(enabled=False)  # Not enabled
            }
        }

        self.mock_vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent
        agent = ChargeAgent(self.mock_session, self.mock_vehicle, [])

        # Assert: Only charging status observers should be registered
        self.assertTrue(agent.chargingStatusObserversRegistered)
        self.assertFalse(agent.plugStatusObserversRegistered)

        # Verify that deferred observer WAS added (because plugStatus not ready)
        mock_weconnect_vehicle.addObserver.assert_called()

    def test_privacy_no_locations_logs_info_message(self):
        """Test that privacy mode logs appropriate message."""
        mock_weconnect_vehicle = self._create_mock_vehicle_with_charging_status(enabled=True)
        self.mock_vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent with privacy enabled
        with patch('vwsfriend.agents.charge_agent.LOG') as mock_log:
            agent = ChargeAgent(self.mock_session, self.mock_vehicle, [Privacy.NO_LOCATIONS])

            # Assert: Privacy info should be logged
            mock_log.info.assert_any_call(
                f'Privacy option \'no-locations\' is set. Vehicle {self.mock_vehicle.vin} will not record charging locations'
            )

    def _create_mock_vehicle_with_charging_status(self, enabled=True):
        """Helper to create a mock WeConnect vehicle with charging status."""
        mock_vehicle = Mock()
        mock_vehicle.statusExists.return_value = True

        charging_status = self._create_mock_charging_status(enabled)
        plug_status = self._create_mock_plug_status(enabled)

        mock_vehicle.domains = {
            'charging': {
                'chargingStatus': charging_status,
                'plugStatus': plug_status
            }
        }

        mock_vehicle.addObserver = Mock()
        mock_vehicle.removeObserver = Mock()

        return mock_vehicle

    def _create_mock_charging_status(self, enabled=True):
        """Helper to create a mock charging status object."""
        status = Mock()
        status.enabled = enabled
        status.carCapturedTimestamp = Mock()
        status.carCapturedTimestamp.addObserver = Mock()
        status.chargingState = Mock()
        status.chargingState.enabled = enabled
        status.chargingState.addObserver = Mock()
        status.chargePower_kW = Mock()
        status.chargePower_kW.addObserver = Mock()
        return status

    def _create_mock_plug_status(self, enabled=True):
        """Helper to create a mock plug status object."""
        status = Mock()
        status.enabled = enabled
        status.plugConnectionState = Mock()
        status.plugConnectionState.addObserver = Mock()
        status.plugLockState = Mock()
        status.plugLockState.addObserver = Mock()
        return status

    def _enable_charging_status(self, mock_vehicle):
        """Helper to enable charging status on a mock vehicle (simulates data becoming available)."""
        mock_vehicle.statusExists.return_value = True
        mock_vehicle.domains['charging']['chargingStatus'].enabled = True
        mock_vehicle.domains['charging']['plugStatus'].enabled = True


if __name__ == '__main__':
    unittest.main()
