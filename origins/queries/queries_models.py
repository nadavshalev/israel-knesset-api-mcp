"""Pydantic models for the unified queries tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument, ItemStage


class QueryResult(KNSBaseModel):
    """A query result (summary or full detail)."""
    # Always present (partial):
    query_id: int = Field(description="Unique query identifier")
    name: str | None = Field(default=None, description="Query name/subject")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Query type description")
    status: str | None = Field(default=None, description="Status description")
    submitter_name: str | None = Field(default=None, description="Submitter name with party")
    gov_ministry_name: str | None = Field(default=None, description="Government ministry the query is directed to")
    session_id: int | None = Field(default=None, description="Most recent plenum session ID where query was discussed")
    last_update_date: str | None = Field(default=None, description="Last updated date (YYYY-MM-DD)")
    # Full detail only (None when partial):
    stages: list[ItemStage] | None = Field(default=None, description="Session stages (plenum appearances) in chronological order (only when full_details=True)")
    submit_date: str | None = Field(default=None, description="Submission date (only when full_details=True)")
    gov_ministry_id: int | None = Field(default=None, description="Government ministry ID (only when full_details=True)")
    reply_minister_date: str | None = Field(default=None, description="Minister reply date (only when full_details=True)")
    reply_date_planned: str | None = Field(default=None, description="Planned reply date (only when full_details=True)")
    documents: list[SessionDocument] | None = Field(default=None, description="Query documents (only when full_details=True)")


class QueriesResults(KNSBaseModel):
    """Results from queries tool."""
    items: list[QueryResult] = Field(description="List of query results")
