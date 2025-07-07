"""
Dashboard configuration module.
"""

from src.utils.logging import setup_logger

# Set up logger
logger = setup_logger(__name__)


def generate_dashboard_config():
    """
    Generate Grafana dashboard configuration.

    Returns:
        dict: Dashboard configuration
    """
    dashboard = {
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": "-- Grafana --",
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        "editable": True,
        "gnetId": None,
        "graphTooltip": 0,
        "id": None,
        "links": [],
        "panels": [
            {
                "aliasColors": {},
                "bars": False,
                "dashLength": 10,
                "dashes": False,
                "datasource": "Prometheus",
                "fieldConfig": {"defaults": {"custom": {}}, "overrides": []},
                "fill": 1,
                "fillGradient": 0,
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "hiddenSeries": False,
                "id": 1,
                "legend": {
                    "avg": False,
                    "current": False,
                    "max": False,
                    "min": False,
                    "show": True,
                    "total": False,
                    "values": False,
                },
                "lines": True,
                "linewidth": 1,
                "nullPointMode": "null",
                "options": {"alertThreshold": True},
                "percentage": False,
                "pluginVersion": "7.2.0",
                "pointradius": 2,
                "points": False,
                "renderer": "flot",
                "seriesOverrides": [],
                "spaceLength": 10,
                "stack": False,
                "steppedLine": False,
                "targets": [
                    {
                        "expr": "sum(rate(records_processed_total[5m])) by (dataset, status)",
                        "interval": "",
                        "legendFormat": "{{dataset}} - {{status}}",
                        "refId": "A",
                    }
                ],
                "thresholds": [],
                "timeFrom": None,
                "timeRegions": [],
                "timeShift": None,
                "title": "Records Processed Rate",
                "tooltip": {"shared": True, "sort": 0, "value_type": "individual"},
                "type": "graph",
                "xaxis": {
                    "buckets": None,
                    "mode": "time",
                    "name": None,
                    "show": True,
                    "values": [],
                },
                "yaxes": [
                    {
                        "format": "short",
                        "label": None,
                        "logBase": 1,
                        "max": None,
                        "min": None,
                        "show": True,
                    },
                    {
                        "format": "short",
                        "label": None,
                        "logBase": 1,
                        "max": None,
                        "min": None,
                        "show": True,
                    },
                ],
                "yaxis": {"align": False, "alignLevel": None},
            },
            {
                "aliasColors": {},
                "bars": False,
                "dashLength": 10,
                "dashes": False,
                "datasource": "Prometheus",
                "fieldConfig": {"defaults": {"custom": {}}, "overrides": []},
                "fill": 1,
                "fillGradient": 0,
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                "hiddenSeries": False,
                "id": 2,
                "legend": {
                    "avg": False,
                    "current": False,
                    "max": False,
                    "min": False,
                    "show": True,
                    "total": False,
                    "values": False,
                },
                "lines": True,
                "linewidth": 1,
                "nullPointMode": "null",
                "options": {"alertThreshold": True},
                "percentage": False,
                "pluginVersion": "7.2.0",
                "pointradius": 2,
                "points": False,
                "renderer": "flot",
                "seriesOverrides": [],
                "spaceLength": 10,
                "stack": False,
                "steppedLine": False,
                "targets": [
                    {
                        "expr": "histogram_quantile(0.95, sum(rate(processing_time_seconds_bucket[5m])) by (dataset, operation, le))",
                        "interval": "",
                        "legendFormat": "{{dataset}} - {{operation}}",
                        "refId": "A",
                    }
                ],
                "thresholds": [],
                "timeFrom": None,
                "timeRegions": [],
                "timeShift": None,
                "title": "Processing Time (95th percentile)",
                "tooltip": {"shared": True, "sort": 0, "value_type": "individual"},
                "type": "graph",
            },
        ],
        "refresh": "5s",
        "schemaVersion": 26,
        "style": "dark",
        "tags": [],
        "templating": {"list": []},
        "time": {"from": "now-6h", "to": "now"},
        "timepicker": {},
        "timezone": "",
        "title": "Data Pipeline Monitoring",
        "uid": None,
        "version": 1,
    }
    return dashboard
