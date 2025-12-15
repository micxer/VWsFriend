"""Simplified tests for ChargeAgent adaptive observer registration.

These tests focus on verifying the adaptive registration logic by using
an in-memory SQLite database to avoid complex SQLAlchemy mocking issues.
"""
import unittest
from unittest.mock import Mock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vwsfriend.model.base import Base
from vwsfriend.model.vehicle import Vehicle
from vwsfriend.agents.charge_agent import ChargeAgent
from vwsfriend.privacy import Privacy
from weconnect.addressable import AddressableLeaf


class TestChargeAgentAdaptiveRegistration(unittest.TestCase):
    """Test ChargeAgent's adaptive observer registration using real database."""

    def setUp(self):
        """Set up test database and session."""
        # Create in-memory SQLite database
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Create a test vehicle
        self.vehicle = Vehicle(vin="WVWZZZE1ZMP1234567")
        self.session.add(self.vehicle)
        self.session.commit()

    def tearDown(self):
        """Clean up database."""
        self.session.close()

    def test_observers_registered_when_charging_status_available(self):
        """Test that observers are registered immediately when charging status is available."""
        # Setup: Create mock WeConnect vehicle with charging status enabled
        mock_weconnect_vehicle = self._create_mock_vehicle(
            charging_status_enabled=True,
            plug_status_enabled=True
        )
        self.vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent
        agent = ChargeAgent(self.session, self.vehicle, [])

        # Assert: Observers should be registered
        self.assertTrue(agent.chargingStatusObserversRegistered)
        self.assertTrue(agent.plugStatusObserversRegistered)

        # Verify observers were added
        charging_status = mock_weconnect_vehicle.domains['charging']['chargingStatus']
        self.assertTrue(charging_status.carCapturedTimestamp.addObserver.called)
        self.assertTrue(charging_status.chargingState.addObserver.called)

        plug_status = mock_weconnect_vehicle.domains['charging']['plugStatus']
        self.assertTrue(plug_status.plugConnectionState.addObserver.called)

        # Verify deferred observer was NOT added (not needed)
        self.assertFalse(mock_weconnect_vehicle.addObserver.called)

    def test_deferred_observer_added_when_status_not_available(self):
        """Test that deferred observer is registered when charging status is not available."""
        # Setup: Create mock WeConnect vehicle with charging status disabled
        mock_weconnect_vehicle = self._create_mock_vehicle(
            charging_status_enabled=False,
            plug_status_enabled=False
        )
        self.vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act: Initialize ChargeAgent
        agent = ChargeAgent(self.session, self.vehicle, [])

        # Assert: Observers should NOT be registered yet
        self.assertFalse(agent.chargingStatusObserversRegistered)
        self.assertFalse(agent.plugStatusObserversRegistered)

        # Verify deferred observer WAS added
        mock_weconnect_vehicle.addObserver.assert_called_once()
        call_args = mock_weconnect_vehicle.addObserver.call_args
        self.assertEqual(call_args[0][1], AddressableLeaf.ObserverEvent.UPDATED_FROM_SERVER)

    def test_deferred_observer_registers_when_status_becomes_available(self):
        """Test that observers are registered when status becomes available later."""
        # Setup: Start with disabled status
        mock_weconnect_vehicle = self._create_mock_vehicle(
            charging_status_enabled=False,
            plug_status_enabled=False
        )
        self.vehicle.weConnectVehicle = mock_weconnect_vehicle

        # Act 1: Initialize (status not available)
        agent = ChargeAgent(self.session, self.vehicle, [])
        self.assertFalse(agent.chargingStatusObserversRegistered)

        # Capture the deferred callback
        deferred_callback = mock_weconnect_vehicle.addObserver.call_args[0][0]

        # Act 2: Enable status and trigger callback
        self._enable_status(mock_weconnect_vehicle)
        deferred_callback(None, None)

        # Assert: Observers should now be registered
        self.assertTrue(agent.chargingStatusObserversRegistered)
        self.assertTrue(agent.plugStatusObserversRegistered)

        # Verify deferred observer was removed
        mock_weconnect_vehicle.removeObserver.assert_called_once()

    def _create_mock_vehicle(self, charging_status_enabled=True, plug_status_enabled=True):
        """Create a mock WeConnect vehicle."""
        mock_vehicle = Mock()
        mock_vehicle.statusExists.return_value = True

        # Create mock charging status
        charging_status = Mock()
        charging_status.enabled = charging_status_enabled
        charging_status.carCapturedTimestamp = Mock()
        charging_status.carCapturedTimestamp.addObserver = Mock()
        charging_status.chargingState = Mock()
        charging_status.chargingState.enabled = charging_status_enabled
        charging_status.chargingState.addObserver = Mock()
        charging_status.chargePower_kW = Mock()
        charging_status.chargePower_kW.addObserver = Mock()

        # Create mock plug status
        plug_status = Mock()
        plug_status.enabled = plug_status_enabled
        plug_status.plugConnectionState = Mock()
        plug_status.plugConnectionState.addObserver = Mock()
        plug_status.plugLockState = Mock()
        plug_status.plugLockState.addObserver = Mock()

        mock_vehicle.domains = {
            'charging': {
                'chargingStatus': charging_status,
                'plugStatus': plug_status
            }
        }

        mock_vehicle.addObserver = Mock()
        mock_vehicle.removeObserver = Mock()

        return mock_vehicle

    def _enable_status(self, mock_vehicle):
        """Enable charging and plug status on a mock vehicle."""
        mock_vehicle.domains['charging']['chargingStatus'].enabled = True
        mock_vehicle.domains['charging']['plugStatus'].enabled = True


if __name__ == '__main__':
    unittest.main()
