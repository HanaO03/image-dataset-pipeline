"""Source registry — the one place that knows which adapters exist."""

from __future__ import annotations

from ..config import SourceSettings
from ..http.client import HttpClient
from .base import ImageSource
from .openverse import OpenverseSource
from .wikimedia import WikimediaCommonsSource

__all__ = ["ImageSource", "OpenverseSource", "WikimediaCommonsSource", "build_sources"]


def build_sources(client: HttpClient, settings: SourceSettings) -> list[ImageSource]:
    """
    Instantiate every configured source.

    Order matters slightly: the API source runs first so that when the same
    photograph exists in both places, the record we keep is the one carrying
    structured licence metadata rather than the one scraped out of HTML.
    """
    return [
        OpenverseSource(client, settings),
        WikimediaCommonsSource(client, settings),
    ]
