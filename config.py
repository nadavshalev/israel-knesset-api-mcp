"""Centralized configuration for all components.

Reads settings from environment variables. A ``.env`` file in the project
root is loaded automatically if python-dotenv is installed.

Required variables (no defaults — server raises RuntimeError at startup if missing):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

All other variables are optional and have sensible defaults.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # dotenv not installed — use environment variables directly


def _require_env(name: str) -> str:
    """Return env var value or raise clearly if not set."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            "Copy .env.example to .env and fill in your values."
        )
    return value


# ---------------------------------------------------------------------------
# PostgreSQL (required — no silent defaults)
# ---------------------------------------------------------------------------

POSTGRES_HOST = _require_env("POSTGRES_HOST")
POSTGRES_PORT = int(_require_env("POSTGRES_PORT"))
POSTGRES_DB = _require_env("POSTGRES_DB")
POSTGRES_USER = _require_env("POSTGRES_USER")
POSTGRES_PASSWORD = _require_env("POSTGRES_PASSWORD")

# ---------------------------------------------------------------------------
# Knesset OData API (hardcoded — not user-configurable)
# ---------------------------------------------------------------------------

BASE_URL = "https://knesset.gov.il/OdataV4/ParliamentInfo/"
ODATA_PAGE_SIZE = 200

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_ENDPOINT = os.getenv("MCP_ENDPOINT", "/mcp")

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))

# ---------------------------------------------------------------------------
# Response limits
# ---------------------------------------------------------------------------

MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "50000"))
SEARCH_ACROSS_TOP_N = int(os.getenv("SEARCH_ACROSS_TOP_N", "5"))
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "50"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "200"))
# Max rows returned by a single detail sub-query (initiators, documents, stages, etc.)
MAX_DETAIL_ITEMS = int(os.getenv("MAX_DETAIL_ITEMS", "200"))

# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

POOL_MAX_CONN = int(os.getenv("POOL_MAX_CONN", "10"))
# Seconds between pool liveness checks (0 = disabled)
POOL_HEALTH_INTERVAL = int(os.getenv("POOL_HEALTH_INTERVAL", "30"))

# ---------------------------------------------------------------------------
# Fuzzy search
# ---------------------------------------------------------------------------

# Trigram similarity threshold — must stay in sync with
# pg_trgm.strict_word_similarity_threshold GUC (set per connection in db.py).
FUZZY_TRGM_THRESHOLD = float(os.getenv("FUZZY_TRGM_THRESHOLD", "0.5"))

# ---------------------------------------------------------------------------
# Tool timeout
# ---------------------------------------------------------------------------

# Max seconds a single MCP tool call may run before returning a timeout error.
# Set to 0 to disable.
MCP_TOOL_TIMEOUT = float(os.getenv("MCP_TOOL_TIMEOUT", "5"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "")  # optional file path; empty = stderr only

# ---------------------------------------------------------------------------
# Updater worker (read by docker/updater-entrypoint.sh and updater scripts)
# ---------------------------------------------------------------------------

UPDATE_CYCLE_DAYS = int(os.getenv("UPDATE_CYCLE_DAYS", "1"))
UPDATE_HOUR_IN_DAY = int(os.getenv("UPDATE_HOUR_IN_DAY", "3"))
UPDATE_RUN_ON_START = os.getenv("UPDATE_RUN_ON_START", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# WireGuard VPN (required only in the updater container; empty = no VPN)
# ---------------------------------------------------------------------------

WG_PRIVATE_KEY = os.getenv("WG_PRIVATE_KEY", "")
WG_PUBLIC_KEY = os.getenv("WG_PUBLIC_KEY", "")
WG_ENDPOINT = os.getenv("WG_ENDPOINT", "")
WG_ADDRESS = os.getenv("WG_ADDRESS", "10.2.0.2/32")
