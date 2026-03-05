"""Per-IP rate limiting ASGI middleware.

Tracks request counts per client IP using a simple in-memory dict with
a sliding-window approach (per-minute buckets).  No external dependencies.
"""

import time
from collections import defaultdict


class RateLimitMiddleware:
    """ASGI middleware that limits requests per IP per minute.

    Parameters
    ----------
    app : ASGI application
        The wrapped ASGI app.
    max_per_minute : int
        Maximum requests allowed per IP per minute.
    """

    def __init__(self, app, max_per_minute: int = 60):
        self.app = app
        self.max_per_minute = max_per_minute
        # {ip: [(timestamp, ...),]}  — list of request timestamps
        self._requests: dict[str, list[float]] = defaultdict(list)

    def _get_client_ip(self, scope: dict) -> str:
        """Extract client IP from ASGI scope."""
        # Check for X-Forwarded-For in headers (reverse proxy)
        headers = dict(scope.get("headers", []))
        forwarded = headers.get(b"x-forwarded-for")
        if forwarded:
            return forwarded.decode().split(",")[0].strip()

        client = scope.get("client")
        if client:
            return client[0]
        return "unknown"

    def _cleanup(self, ip: str, now: float) -> None:
        """Remove timestamps older than 60 seconds."""
        cutoff = now - 60.0
        timestamps = self._requests[ip]
        # Find first index that is within the window
        i = 0
        while i < len(timestamps) and timestamps[i] < cutoff:
            i += 1
        if i > 0:
            self._requests[ip] = timestamps[i:]

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        ip = self._get_client_ip(scope)
        now = time.monotonic()
        self._cleanup(ip, now)

        if len(self._requests[ip]) >= self.max_per_minute:
            # Rate limit exceeded — return 429
            response_body = b'{"error": "Rate limit exceeded. Try again later."}'
            await send({
                "type": "http.response.start",
                "status": 429,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"retry-after", b"60"],
                ],
            })
            await send({
                "type": "http.response.body",
                "body": response_body,
            })
            return

        self._requests[ip].append(now)
        await self.app(scope, receive, send)
