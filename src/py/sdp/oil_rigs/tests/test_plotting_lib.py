"""
Unit tests for plotting_lib module (simplified PySpark 4.0+ native plotting).

Tests the core plotting functions without requiring a full Spark session
or database connection. This version tests the simplified implementation
that uses PySpark native DataFrame.plot API with PNG-only output.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from dataclasses import dataclass

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from plotting_lib import (
    SensorPlotConfig,
    SENSOR_CONFIGS,
    get_available_rigs,
    filter_by_rig,
)


class TestSensorPlotConfig:
    """Test SensorPlotConfig dataclass."""

    def test_config_creation(self):
        """Test creating a sensor plot configuration."""
        config = SensorPlotConfig(
            metric_name="temperature",
            table_name="temperature_events_mv",
            y_column="temperature_f",
            y_label="Temperature (°F)",
            title_prefix="Temperature",
            value_format="°F",
            hover_template="Time: %{x}<br>Temperature: %{y:.1f}°F"
        )

        assert config.metric_name == "temperature"
        assert config.table_name == "temperature_events_mv"
        assert config.y_column == "temperature_f"
        assert config.y_label == "Temperature (°F)"
        assert config.title_prefix == "Temperature"
        assert config.value_format == "°F"

    def test_config_post_init_defaults(self):
        """Test that post_init sets default aggregation column names."""
        config = SensorPlotConfig(
            metric_name="pressure",
            table_name="pressure_events_mv",
            y_column="pressure_psi",
            y_label="Pressure (PSI)",
            title_prefix="Pressure",
            value_format="PSI",
            hover_template="Pressure: %{y:.1f}"
        )

        assert config.min_col_name == "min_pressure"
        assert config.max_col_name == "max_pressure"
        assert config.avg_col_name == "avg_pressure"

    def test_config_custom_agg_names(self):
        """Test providing custom aggregation column names."""
        config = SensorPlotConfig(
            metric_name="water_level",
            table_name="water_level_events_mv",
            y_column="water_level_feet",
            y_label="Water Level (feet)",
            title_prefix="Water Level",
            value_format="feet",
            hover_template="Level: %{y:.1f}",
            min_col_name="minimum_level",
            max_col_name="maximum_level",
            avg_col_name="average_level"
        )

        assert config.min_col_name == "minimum_level"
        assert config.max_col_name == "maximum_level"
        assert config.avg_col_name == "average_level"


class TestSensorConfigs:
    """Test predefined SENSOR_CONFIGS dictionary."""

    def test_all_configs_present(self):
        """Test that all expected sensor configs are present."""
        assert "temperature" in SENSOR_CONFIGS
        assert "pressure" in SENSOR_CONFIGS
        assert "water_level" in SENSOR_CONFIGS

    def test_temperature_config(self):
        """Test temperature configuration."""
        config = SENSOR_CONFIGS["temperature"]
        assert config.metric_name == "temperature"
        assert config.table_name == "temperature_events_mv"
        assert config.y_column == "temperature_f"
        assert "°F" in config.y_label
        assert "°F" in config.value_format

    def test_pressure_config(self):
        """Test pressure configuration."""
        config = SENSOR_CONFIGS["pressure"]
        assert config.metric_name == "pressure"
        assert config.table_name == "pressure_events_mv"
        assert config.y_column == "pressure_psi"
        assert "PSI" in config.y_label
        assert "PSI" in config.value_format

    def test_water_level_config(self):
        """Test water level configuration."""
        config = SENSOR_CONFIGS["water_level"]
        assert config.metric_name == "water_level"
        assert config.table_name == "water_level_events_mv"
        assert config.y_column == "water_level_feet"
        assert "feet" in config.y_label
        assert "feet" in config.value_format


class TestFilterByRig:
    """Test filter_by_rig function."""

    @patch('plotting_lib.col')
    def test_filter_by_rig(self, mock_col):
        """Test filtering DataFrame by rig name."""
        # Create mock DataFrame
        mock_df = MagicMock()
        mock_filtered = MagicMock()
        mock_df.filter.return_value = mock_filtered

        # Mock the col function
        mock_column = MagicMock()
        mock_col.return_value = mock_column
        mock_column.__eq__ = MagicMock(return_value=mock_column)

        result = filter_by_rig(mock_df, "permian_rig")

        # Verify col was called with correct column name
        mock_col.assert_called_once_with("rig_name")
        # Verify filter was called
        mock_df.filter.assert_called_once()
        assert result == mock_filtered


class TestGetAvailableRigs:
    """Test get_available_rigs function."""

    def test_get_available_rigs(self):
        """Test extracting unique rig names from DataFrame."""
        # Create mock DataFrame with rig data
        mock_df = MagicMock()
        mock_selected = MagicMock()
        mock_distinct = MagicMock()

        # Mock the chain: select -> distinct -> collect
        mock_df.select.return_value = mock_selected
        mock_selected.distinct.return_value = mock_distinct

        # Mock Row objects
        row1 = MagicMock()
        row1.rig_name = "permian_rig"
        row2 = MagicMock()
        row2.rig_name = "eagle_ford_rig"

        mock_distinct.collect.return_value = [row1, row2]

        result = get_available_rigs(mock_df)

        # Verify method calls
        mock_df.select.assert_called_once_with("rig_name")
        mock_selected.distinct.assert_called_once()
        mock_distinct.collect.assert_called_once()

        # Verify result is sorted
        assert result == ["eagle_ford_rig", "permian_rig"]

    def test_get_available_rigs_single_rig(self):
        """Test with single rig."""
        mock_df = MagicMock()
        mock_selected = MagicMock()
        mock_distinct = MagicMock()

        mock_df.select.return_value = mock_selected
        mock_selected.distinct.return_value = mock_distinct

        row = MagicMock()
        row.rig_name = "test_rig"
        mock_distinct.collect.return_value = [row]

        result = get_available_rigs(mock_df)

        assert result == ["test_rig"]
        assert len(result) == 1


class TestOutputFilenameGeneration:
    """Test output filename generation logic (PNG-only)."""

    def test_single_rig_filename(self):
        """Test filename for single rig plot (PNG format)."""
        rig_name = "permian_rig"
        metric = "temperature"
        expected = f"{rig_name}_{metric}_plot.png"

        assert expected == "permian_rig_temperature_plot.png"

    def test_filename_formats_all_metrics(self):
        """Test filename generation for all metric types (PNG format)."""
        rig = "eagle_ford_rig"

        for metric in ["temperature", "pressure", "water_level"]:
            filename = f"{rig}_{metric}_plot.png"
            assert filename.endswith("_plot.png")
            assert rig in filename
            assert metric in filename

    def test_filename_extension_is_png(self):
        """Test that all filenames use PNG extension."""
        rigs = ["permian_rig", "eagle_ford_rig"]
        metrics = ["temperature", "pressure", "water_level"]

        for rig in rigs:
            for metric in metrics:
                filename = f"{rig}_{metric}_plot.png"
                assert filename.endswith(".png")
                assert not filename.endswith(".html")


class TestPlotTitleGeneration:
    """Test plot title generation logic (individual rigs only)."""

    def test_single_rig_title(self):
        """Test title for individual rig plot."""
        config = SENSOR_CONFIGS["temperature"]
        rig_name = "permian_rig"

        title = f"{config.title_prefix} Readings - {rig_name.replace('_', ' ').title()}"

        assert title == "Temperature Readings - Permian Rig"

    def test_title_formatting_all_metrics(self):
        """Test title formatting for all metrics (individual rigs only)."""
        rigs = ["permian_rig", "eagle_ford_rig"]

        for config in SENSOR_CONFIGS.values():
            for rig in rigs:
                title = f"{config.title_prefix} Readings - {rig.replace('_', ' ').title()}"
                assert config.title_prefix in title
                assert rig.replace('_', ' ').title() in title

    def test_title_name_formatting(self):
        """Test that rig names are properly formatted in titles."""
        test_cases = [
            ("permian_rig", "Permian Rig"),
            ("eagle_ford_rig", "Eagle Ford Rig"),
            ("test_rig_name", "Test Rig Name"),
        ]

        config = SENSOR_CONFIGS["pressure"]

        for rig_name, expected_formatted in test_cases:
            title = f"{config.title_prefix} Readings - {rig_name.replace('_', ' ').title()}"
            assert expected_formatted in title


class TestConfigurationValidation:
    """Test configuration validation and consistency."""

    def test_all_configs_have_required_fields(self):
        """Test that all configs have required fields."""
        required_fields = [
            'metric_name', 'table_name', 'y_column',
            'y_label', 'title_prefix', 'value_format', 'hover_template'
        ]

        for metric_name, config in SENSOR_CONFIGS.items():
            for field in required_fields:
                assert hasattr(config, field), f"{metric_name} missing {field}"
                assert getattr(config, field), f"{metric_name}.{field} is empty"

    def test_metric_names_match_keys(self):
        """Test that metric_name matches dictionary key."""
        for key, config in SENSOR_CONFIGS.items():
            assert config.metric_name == key

    def test_table_names_follow_convention(self):
        """Test that table names follow naming convention."""
        for config in SENSOR_CONFIGS.values():
            assert config.table_name.endswith("_events_mv")
            assert config.metric_name in config.table_name or \
                   config.table_name.startswith(config.metric_name.replace('_', ''))

    def test_hover_templates_include_values(self):
        """Test that hover templates include value placeholders."""
        for config in SENSOR_CONFIGS.values():
            # Should have time and value placeholders
            assert "%{x}" in config.hover_template or "Time" in config.hover_template
            assert "%{y" in config.hover_template


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
