"""Integration test for core/vpn.py — verifies IP changes while VPN is active."""

import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.vpn import vpn_connection


def _get_current_ip() -> str:
    """Return the public IP address, or raise on failure."""
    return requests.get("https://api.ipify.org", timeout=15).text.strip()


def test_vpn_changes_ip():
    """IP during VPN should differ from the original IP,
    and should revert after the VPN context exits."""
    ip_before = _get_current_ip()

    with vpn_connection():
        ip_during = _get_current_ip()

    ip_after = _get_current_ip()

    assert ip_during != ip_before, (
        f"IP did not change during VPN — still {ip_before}"
    )
    assert ip_after == ip_before, (
        f"IP did not revert after VPN — expected {ip_before}, got {ip_after}"
    )
