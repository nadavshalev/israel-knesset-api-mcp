import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # dotenv not installed — use environment variables or defaults


DEFAULT_DB = Path(os.getenv("DB_PATH", "data.sqlite"))
BASE_URL = "https://knesset.gov.il/OdataV4/ParliamentInfo/"
DEFAULT_PAGE_SIZE = 200
MAX_KNESSET_NUM = 30

# MCP Server
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_ENDPOINT = os.getenv("MCP_ENDPOINT", "/mcp")

# Rate limiting
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))

# Response limits
MAX_RESULTS_SIZE = int(os.getenv("MAX_RESULTS_SIZE", "50000"))
SEARCH_ACROSS_TOP_N = int(os.getenv("SEARCH_ACROSS_TOP_N", "5"))
