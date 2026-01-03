"""Superset configuration for Open Data Stack."""

import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Flask App Builder configuration
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "open-data-stack-secret-key-change-in-prod")

# Database configuration - use SQLite for metadata
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# CSV export settings
CSV_EXPORT = {
    "encoding": "utf-8",
}

# Enable SQL Lab
ENABLE_PROXY_FIX = True

# Default database for SQL Lab
SQLLAB_DEFAULT_DBID = 1

# Async query settings (disabled for simplicity)
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300

# Theme
APP_NAME = "Open Data Stack"
