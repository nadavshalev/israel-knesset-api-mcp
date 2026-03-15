"""Base Pydantic model for all Knesset data models.

Provides ``KNSBaseModel`` which automatically strips empty/sentinel values
(``None``, ``""``, ``-1``) from serialized output via a custom
``@model_serializer``.  This ensures that no matter *who* calls
``model_dump()`` — our handler, FastMCP's ``convert_result``, etc. — the
output is always clean.
"""

from __future__ import annotations

from pydantic import BaseModel, model_serializer

from core.helpers import clean


class KNSBaseModel(BaseModel):
    """Base model that strips empty values on serialization."""

    @model_serializer(mode="wrap")
    def _clean_serializer(self, handler):
        data = handler(self)
        return clean(data)
