"""WireGuard VPN connection helper.

Provides a context manager that establishes a WireGuard tunnel for the
duration of its block, ensuring the connection is torn down on exit.

Usage::

    from core.vpn import vpn_connection

    with vpn_connection():
        # all network traffic inside this block goes through the VPN
        requests.get("https://example.com")
"""

import io
import os
import socket as stdlib_socket
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from wireguard_requests import Peer, WireGuardConfig, wireguard_context

# Ensure .env is loaded (idempotent if already loaded elsewhere)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _build_config() -> WireGuardConfig:
    """Build a WireGuardConfig from environment variables."""
    private_key = os.getenv("WG_PRIVATE_KEY")
    public_key = os.getenv("WG_PUBLIC_KEY")
    endpoint = os.getenv("WG_ENDPOINT")
    address = os.getenv("WG_ADDRESS", "10.2.0.2/32")

    missing = []
    if not private_key:
        missing.append("WG_PRIVATE_KEY")
    if not public_key:
        missing.append("WG_PUBLIC_KEY")
    if not endpoint:
        missing.append("WG_ENDPOINT")
    if missing:
        raise RuntimeError(
            f"Missing required WireGuard env vars: {', '.join(missing)}"
        )

    # address may include prefix length (e.g. "10.2.0.2/32")
    if "/" in address:
        addr, prefix = address.split("/", 1)
        prefix_len = int(prefix)
    else:
        addr = address
        prefix_len = 32

    return WireGuardConfig(
        private_key=private_key,
        mtu=1280,
        peers=[
            Peer(
                public_key=public_key,
                endpoint=endpoint,
                allowed_ips=["0.0.0.0/0"],
            )
        ],
        address=addr,
        prefix_len=prefix_len,
    )


def _patch_wireguard_bugs():
    """Fix bugs in wireguard_requests' socket wrappers.

    **Bug 1 — makefile buffering:**
    Both ``WireGuardSocket.makefile()`` and ``WireGuardTlsSocket.makefile()``
    incorrectly return an unbuffered raw wrapper when ``buffering=-1``
    (the default).  The condition::

        if buffering == 0 or ("b" in mode and buffering < 0):
            return raw

    treats ``buffering=-1`` ("use default") the same as ``buffering=0``
    ("unbuffered").  This means ``read(N)`` can return fewer than N bytes
    (a single TLS record), which ``http.client._safe_read()`` interprets
    as ``IncompleteRead``.

    **Bug 2 — connect timeout:**
    ``WireGuardSocket.connect()`` never propagates the stored ``_timeout``
    to the newly created ``WgStream``.  When urllib3 calls
    ``sock.settimeout(60)`` *before* ``sock.connect()``, the timeout is
    stored but the ``_stream`` is ``None``, so ``set_timeout`` is never
    called.  After ``connect()`` creates the stream, the timeout is not
    applied — causing ``create_stream()`` to block indefinitely on slow
    or unresponsive servers.

    **Bug 3 — recv timeout not enforced:**
    ``WgStream.set_timeout()`` sets a timeout on the Rust side, but it
    is not reliably enforced during ``recv()`` — observed as 10+ hour
    hangs in production.  Furthermore, ``stream.close()`` does NOT
    interrupt a blocked ``recv()``.  We work around this by running each
    ``recv()`` in a disposable daemon thread with a ``join(timeout)``
    — the same pattern used for the connect fix.  If the thread doesn't
    return in time, we abandon it and raise ``socket.timeout``.

    Returns a callable that restores the original methods.
    """
    from wireguard_requests.socket import WireGuardSocket, _SocketFileWrapper
    from wireguard_requests.tls import WireGuardTlsSocket, _TlsFileWrapper

    orig_sock_makefile = WireGuardSocket.makefile
    orig_tls_makefile = WireGuardTlsSocket.makefile
    orig_connect = WireGuardSocket.connect
    orig_recv = WireGuardSocket.recv

    # -- Bug 1 fix: makefile buffering -----------------------------------

    def _fixed_makefile(self, mode="r", buffering=-1, **kwargs):
        if "b" not in mode:
            mode = mode + "b"

        self._makefile_refs += 1

        # Pick the right raw wrapper based on socket type
        if isinstance(self, WireGuardTlsSocket):
            raw = _TlsFileWrapper(self)
        else:
            raw = _SocketFileWrapper(self)

        # Only return unbuffered when explicitly requested (buffering=0)
        if buffering == 0:
            return raw

        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE

        if "r" in mode:
            return io.BufferedReader(raw, buffer_size=buffering)
        elif "w" in mode:
            return io.BufferedWriter(raw, buffer_size=buffering)
        else:
            return io.BufferedRWPair(raw, raw, buffer_size=buffering)

    WireGuardSocket.makefile = _fixed_makefile
    WireGuardTlsSocket.makefile = _fixed_makefile

    # -- Bug 2 fix: connect timeout --------------------------------------

    def _fixed_connect(self, address):
        # Enforce a timeout on create_stream() itself (DNS + TCP handshake
        # through the VPN tunnel).  The original connect() never passes
        # the stored _timeout to the Rust side, so create_stream() can
        # block forever.
        timeout = self._timeout
        t0 = time.monotonic()
        if timeout is not None and timeout > 0:
            exc_holder = []

            def _do_connect():
                try:
                    orig_connect(self, address)
                except Exception as e:
                    exc_holder.append(e)

            t = threading.Thread(target=_do_connect, daemon=True)
            t.start()
            t.join(timeout=timeout)
            if t.is_alive():
                elapsed = time.monotonic() - t0
                print(f"  [vpn] connect to {address} TIMED OUT after {elapsed:.1f}s "
                      f"(timeout={timeout}s)")
                # connect is stuck — close the socket to unblock the thread
                try:
                    self._closed = True
                    if self._stream is not None:
                        self._stream.close()
                except Exception:
                    pass
                raise stdlib_socket.timeout(
                    f"VPN connect to {address} timed out after {timeout}s"
                )
            if exc_holder:
                elapsed = time.monotonic() - t0
                print(f"  [vpn] connect to {address} FAILED after {elapsed:.1f}s: "
                      f"{type(exc_holder[0]).__name__}: {exc_holder[0]}")
                raise exc_holder[0]
        else:
            orig_connect(self, address)

        elapsed = time.monotonic() - t0
        print(f"  [vpn] connected to {address} in {elapsed:.1f}s")

        # Propagate any timeout that was set before connect()
        if timeout is not None and self._stream is not None:
            self._stream.set_timeout(timeout)

    WireGuardSocket.connect = _fixed_connect

    # -- Bug 3 fix: recv timeout enforcement -------------------------------
    #
    # The Rust WgStream.recv() blocks indefinitely even when set_timeout()
    # has been called, and stream.close() does NOT interrupt a blocked
    # recv().  The only reliable way to enforce a timeout is to run recv()
    # in a disposable daemon thread and abandon it on timeout — the same
    # pattern used for _fixed_connect above.
    #
    # Performance: thread creation is ~0.1-1ms on Linux.  A typical HTTP
    # request does 10-40 recv() calls, adding <40ms total — negligible
    # compared to network latency.

    # Hard ceiling so a single recv() can never block longer than this,
    # even if the caller sets an absurdly large timeout.
    _MAX_RECV_TIMEOUT = 120  # seconds

    _recv_diag_logged = False

    def _fixed_recv(self, bufsize, flags=0):
        nonlocal _recv_diag_logged
        timeout = self._timeout

        if not _recv_diag_logged:
            _recv_diag_logged = True
            print(f"  [vpn] _fixed_recv active: timeout={timeout}, "
                  f"stream={self._stream is not None}")

        # No timeout enforcement needed when no timeout or stream is gone.
        if not timeout or timeout <= 0 or self._stream is None:
            return orig_recv(self, bufsize, flags)

        effective = min(timeout, _MAX_RECV_TIMEOUT)
        result_holder = []
        exc_holder = []

        def _do_recv():
            try:
                result_holder.append(orig_recv(self, bufsize, flags))
            except Exception as e:
                exc_holder.append(e)

        t = threading.Thread(target=_do_recv, daemon=True)
        t.start()
        t.join(timeout=effective)

        if t.is_alive():
            # recv is stuck — the daemon thread will be abandoned.
            # Mark the socket as closed so no further operations try to
            # use it; the abandoned thread will eventually be cleaned up
            # when the process exits or the Rust tunnel is torn down.
            print(f"  [vpn] recv TIMED OUT after {effective}s")
            try:
                self._closed = True
            except Exception:
                pass
            raise stdlib_socket.timeout(
                f"VPN recv timed out after {effective}s"
            )

        if exc_holder:
            raise exc_holder[0]
        return result_holder[0]

    WireGuardSocket.recv = _fixed_recv

    # -- Restore callback ------------------------------------------------

    def _restore():
        WireGuardSocket.makefile = orig_sock_makefile
        WireGuardTlsSocket.makefile = orig_tls_makefile
        WireGuardSocket.connect = orig_connect
        WireGuardSocket.recv = orig_recv

    return _restore


@contextmanager
def vpn_connection():
    """Context manager that routes traffic through a WireGuard VPN tunnel.

    Reads configuration from environment variables (WG_PRIVATE_KEY,
    WG_PUBLIC_KEY, WG_ENDPOINT, WG_ADDRESS).  The tunnel is guaranteed to
    be torn down when the block exits, even on exceptions.
    """
    config = _build_config()
    restore_patches = _patch_wireguard_bugs()
    endpoint = os.getenv("WG_ENDPOINT", "?")
    print(f"Connecting to VPN (endpoint={endpoint})...")
    t0 = time.monotonic()
    try:
        with wireguard_context(config) as tunnel:
            elapsed = time.monotonic() - t0
            print(f"VPN connected in {elapsed:.1f}s "
                  f"(tunnel alive={tunnel.is_alive()})")
            # Quick connectivity check — make a tiny OData request through
            # the tunnel to verify end-to-end connectivity before starting
            # the real fetches.  DNS is resolved by the system resolver
            # (not the tunnel), so we test with an HTTP round-trip instead.
            try:
                import requests as _req
                check_t0 = time.monotonic()
                print("VPN health check: requesting...")
                r = _req.get(
                    "https://knesset.gov.il/OdataV4/ParliamentInfo/",
                    timeout=30,
                )
                check_elapsed = time.monotonic() - check_t0
                print(f"VPN health check OK: HTTP {r.status_code} "
                      f"in {check_elapsed:.1f}s")
            except Exception as e:
                check_elapsed = time.monotonic() - check_t0
                print(f"VPN health check FAILED after {check_elapsed:.1f}s: "
                      f"{type(e).__name__}: {e}")
            yield
    finally:
        restore_patches()
    print("VPN disconnected.")
