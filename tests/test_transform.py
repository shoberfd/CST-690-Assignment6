# tests/test_transform.py
import pandas as pd
import pytest
from src.transform import calculate_health_risk

def test_calculate_health_risk():
    # Tests the health risk index calculation under various conditions.

    # 1. Test with normal values
    test_data = pd.DataFrame({
        "pm25": [10, 50],
        "temp": [20, 30] # One below threshold, one above
    })
    expected_risk = pd.Series([(10 * 1.5) + 0, (50 * 1.5) + ((30 - 25) * 2)])
    # Expected: [15.0, 85.0]

    calculated_risk = calculate_health_risk(test_data["pm25"], test_data["temp"])
    pd.testing.assert_series_equal(expected_risk, calculated_risk, check_names=False)

    # 2. Test with zero values
    test_data_zero = pd.DataFrame({"pm25": [0], "temp": [0]})
    expected_risk_zero = pd.Series([0.0])
    calculated_risk_zero = calculate_health_risk(test_data_zero["pm25"], test_data_zero["temp"])
    pd.testing.assert_series_equal(expected_risk_zero, calculated_risk_zero, check_names=False)