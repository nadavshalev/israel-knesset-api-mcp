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


def _patch_makefile_buffering():
    """Fix a buffering bug in wireguard_requests' socket wrappers.

    Both ``WireGuardSocket.makefile()`` and ``WireGuardTlsSocket.makefile()``
    incorrectly return an unbuffered raw wrapper when ``buffering=-1``
    (the default).  The condition::

        if buffering == 0 or ("b" in mode and buffering < 0):
            return raw

    treats ``buffering=-1`` ("use default") the same as ``buffering=0``
    ("unbuffered").  This means ``read(N)`` can return fewer than N bytes
    (a single TLS record), which ``http.client._safe_read()`` interprets
    as ``IncompleteRead``.

    This function monkey-patches both ``makefile()`` methods so that
    ``buffering=-1`` falls through to create an ``io.BufferedReader``,
    matching the behaviour of real sockets.

    Returns a callable that restores the original methods.
    """
    from wireguard_requests.socket import WireGuardSocket, _SocketFileWrapper
    from wireguard_requests.tls import WireGuardTlsSocket, _TlsFileWrapper

    orig_sock_makefile = WireGuardSocket.makefile
    orig_tls_makefile = WireGuardTlsSocket.makefile

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

    def _restore():
        WireGuardSocket.makefile = orig_sock_makefile
        WireGuardTlsSocket.makefile = orig_tls_makefile

    return _restore


@contextmanager
def vpn_connection():
    """Context manager that routes traffic through a WireGuard VPN tunnel.

    Reads configuration from environment variables (WG_PRIVATE_KEY,
    WG_PUBLIC_KEY, WG_ENDPOINT, WG_ADDRESS).  The tunnel is guaranteed to
    be torn down when the block exits, even on exceptions.
    """
    config = _build_config()
    restore_makefile = _patch_makefile_buffering()
    print("Connecting to VPN...")
    try:
        with wireguard_context(config):
            print("VPN connected.")
            yield
    finally:
        restore_makefile()
    print("VPN disconnected.")
