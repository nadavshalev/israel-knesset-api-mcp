# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx"]
# ///
"""Interactive MCP tool-call tester.

Run cell-by-cell in an IDE (VS Code / PyCharm / Jupyter) using ``# %%``
markers, or run the whole file to execute all calls sequentially.

Each section sends one MCP JSON-RPC request to the local server and
prints the response.  The server logs (tool_call / tool_done / tool_error)
appear in the server's terminal or LOG_FILE.

Start the server first::

    python mcp_server.py            # stderr shows Rich-formatted logs
    LOG_FILE=mcp.log python mcp_server.py   # also writes to file
"""
#%%
import json
import httpx

BASE = "http://localhost:8000/mcp"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
_id = 0


def call(method: str, params: dict | None = None):
    global _id
    _id += 1
    body = {"jsonrpc": "2.0", "id": _id, "method": method}
    if params:
        body["params"] = params
    resp = httpx.post(BASE, json=body, headers=HEADERS, timeout=30)
    data = resp.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])
    if len(json.dumps(data, ensure_ascii=False)) > 2000:
        print("... (truncated)")
    return data


# %% 1. Initialize
call("initialize", {
    "protocolVersion": "2025-03-26",
    "capabilities": {},
    "clientInfo": {"name": "test-script", "version": "1.0"},
})

# %% 2. List tools
call("tools/list")

# %% 3. metadata — Knesset 25
call("tools/call", {"name": "metadata", "arguments": {"knesset_num": 25}})

# %% 4. members — search by last name
call("tools/call", {"name": "members", "arguments": {"last_name": "נתניהו", "knesset_num": 25}})

# %% 5. members — detail by ID
call("tools/call", {"name": "members", "arguments": {"member_id": 839}})

# %% 6. bills — search
call("tools/call", {"name": "bills", "arguments": {"knesset_num": 25, "name_query": "חוק-יסוד"}})

# %% 7. votes — search by date range
call("tools/call", {"name": "votes", "arguments": {"knesset_num": 25, "from_date": "2024-01-01", "to_date": "2024-01-31"}})

# %% 8. committees — search
call("tools/call", {"name": "committees", "arguments": {"knesset_num": 25, "from_date": "2024-03-01", "to_date": "2024-03-07"}})

# %% 9. plenums — search
call("tools/call", {"name": "plenums", "arguments": {"knesset_num": 25, "from_date": "2024-03-01", "to_date": "2024-03-07"}})

# %% 10. search_across — broad search
call("tools/call", {"name": "search_across", "arguments": {"query": "תקציב", "knesset_num": 25}})

# %% 11. Error case — missing required filter (should trigger tool_error log)
call("tools/call", {"name": "votes", "arguments": {}})
