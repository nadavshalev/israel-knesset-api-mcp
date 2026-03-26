"""Pydantic models for the unified queries tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument, ItemStage


class QueryResultPartial(KNSBaseModel):
    """A query search result (summary fields only)."""
    query_id: int = Field(description="Unique query identifier")
    name: str | None = Field(default=None, description="Query name/subject")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Query type description")
    status: str | None = Field(default=None, description="Status description")
    submitter_name: str | None = Field(default=None, description="Submitter name with party")
    gov_ministry_name: str | None = Field(default=None, description="Government ministry the query is directed to")
    session_id: int | None = Field(default=None, description="Most recent plenum session ID where query was discussed")
    last_update_date: str | None = Field(default=None, description="Last updated date (YYYY-MM-DD)")


class QueryResultFull(QueryResultPartial):
    """A query full-detail result (summary + detail fields)."""
    stages: list[ItemStage] | None = Field(default=None, description="Session stages (plenum appearances) in chronological order")
    submit_date: str | None = Field(default=None, description="Submission date")
    gov_ministry_id: int | None = Field(default=None, description="Government ministry ID")
    reply_minister_date: str | None = Field(default=None, description="Minister reply date")
    reply_date_planned: str | None = Field(default=None, description="Planned reply date")
    documents: list[SessionDocument] | None = Field(default=None, description="Query documents")


# Backward-compat alias
QueryResult = QueryResultFull


class QueriesResults(KNSBaseModel):
    """Results from queries tool."""
    total_count: int = Field(description="Total matching results (before pagination)")
    items: list[QueryResultPartial | QueryResultFull] = Field(description="List of query results")
