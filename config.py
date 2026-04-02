import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # dotenv not installed — use environment variables or defaults


# PostgreSQL connection parameters
POSTGRES_HOST = os.getenv("POSTGRES_PATH", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

BASE_URL = "https://knesset.gov.il/OdataV4/ParliamentInfo/"
ODATA_PAGE_SIZE = 200

# MCP Server
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_ENDPOINT = os.getenv("MCP_ENDPOINT", "/mcp")

# Rate limiting
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))

# Response limits
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "50000"))
SEARCH_ACROSS_TOP_N = int(os.getenv("SEARCH_ACROSS_TOP_N", "5"))
MAX_SEARCH_RESULTS = int(os.getenv("MAX_SEARCH_RESULTS", "300"))
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "50"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "200"))

# Connection pool
POOL_MAX_CONN = int(os.getenv("POOL_MAX_CONN", "10"))

# Fuzzy search — trigram similarity threshold.
# Must stay in sync with pg_trgm.strict_word_similarity_threshold GUC (set on each connection).
FUZZY_TRGM_THRESHOLD = float(os.getenv("FUZZY_TRGM_THRESHOLD", "0.5"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "")  # optional path; when set, logs also go to this file

# Tool timeout — max seconds a single MCP tool call may run before returning an error.
# Set to 0 to disable.
MCP_TOOL_TIMEOUT = float(os.getenv("MCP_TOOL_TIMEOUT", "2.0"))