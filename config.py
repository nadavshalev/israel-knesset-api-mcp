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
DEFAULT_PAGE_SIZE = 200

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
