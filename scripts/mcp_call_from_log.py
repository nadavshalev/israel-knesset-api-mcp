import ast
import json
import httpx

BASE = "http://localhost:8000/mcp"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
_id = 0

MAX_PRINT_CHARS = 6000


def _print_result(method: str, data: dict):
    """Pretty-print an MCP response."""
    sep = "─" * 60

    # Extract the inner result payload
    result = data.get("result", {})
    error = data.get("error")

    if error:
        print(f"\n❌  ERROR  [{method}]")
        print(sep)
        print(json.dumps(error, indent=2, ensure_ascii=False))
        print(sep)
        return

    # tools/call wraps the real payload in result.content[0].text (JSON string)
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        try:
            payload = json.loads(content[0]["text"])
        except (json.JSONDecodeError, KeyError):
            payload = result
    else:
        payload = result

    print(f"\n✅  {method}")
    print(sep)

    rendered = json.dumps(payload, indent=2, ensure_ascii=False)
    if len(rendered) > MAX_PRINT_CHARS:
        print(rendered[:MAX_PRINT_CHARS])
        print(f"\n... [{len(rendered) - MAX_PRINT_CHARS:,} chars truncated]")
    else:
        print(rendered)
    print(sep)


def call(method: str, params: dict | None = None):
    global _id
    _id += 1
    body = {"jsonrpc": "2.0", "id": _id, "method": method}
    if params:
        body["params"] = params
    resp = httpx.post(BASE, json=body, headers=HEADERS, timeout=30)
    data = resp.json()
    _print_result(method, data)
    return data


def tool_call(name: str, arguments: dict | None = None):
    print(f"\n→ tool_call: {name}  args={json.dumps(arguments or {}, ensure_ascii=False)}")
    return call("tools/call", {"name": name, "arguments": arguments or {}})


def tool_from_str(call_str: str):
    """Replay a tool call from a log line.

    Example log line:
        INFO  tool_call: committees  params={'from_date': '2024-10-01', ...}  mcp_server.py:332
    """
    prefix = "tool_call: "
    if prefix not in call_str:
        raise ValueError(f"Invalid call string: {call_str!r}")
    after_prefix = call_str.split(prefix, 1)[1].strip()
    tool_name, params_part = after_prefix.split("  params=", 1)
    tool_name = tool_name.strip()
    params_str = params_part.strip().rsplit("}", 1)[0].strip() + "}"
    arguments = ast.literal_eval(params_str)
    return tool_call(tool_name, arguments)


if __name__ == "__main__":
    # print("Paste a log row and press Enter. Ctrl+C to exit.")
    # while True:
    #     try:
    #         line = input("> ").strip()
    #         if not line:
    #             continue
    #         tool_from_str(line)
    #     except KeyboardInterrupt:
    #         print("\nBye.")
    #         break
    #     except Exception as e:
    #         print(f"Error: {e}")
    params =   {
        "from_date": "2026-01-01",
        "to_date": "2026-01-31",
        "status": "התקבלה בקריאה שלישית",
        "top": 200
    }

    print(tool_call("bills", params))
