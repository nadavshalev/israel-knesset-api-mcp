"""Pydantic models for search_across_view outputs."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from core.models import KNSBaseModel


class EntityResult(KNSBaseModel):
    """Search results for a single entity type."""
    count: int = Field(description="Total matches for this entity type")
    top: list[dict[str, Any]] = Field(default_factory=list, description="Top N results (fields vary by entity type)")


class SearchAcrossResults(KNSBaseModel):
    """Results from search_across."""
    query: str = Field(description="The search term that was used")
    results: dict[str, EntityResult] = Field(default_factory=dict, description="Keyed by entity type (members, bills, committees, votes, plenums)")
