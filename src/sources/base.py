"""
The source abstraction.

Both collectors — a JSON API and an HTML scraper — sit behind one interface:

    fetch(class_label, limit) -> Iterator[RawRecord]

Everything downstream (download, validate, dedupe, normalise, split, export) is
written against `RawRecord` and has no idea where a record came from. Adding a
third source is a new file in this package plus one line in the registry; no
pipeline stage changes. That is the property worth defending in a review.

Two rules every adapter follows:

1. **Yield, don't return.** Fetching is the slow, failure-prone part. A
   generator lets the caller stop at `limit` without over-fetching, and keeps
   memory flat regardless of how much upstream offers.

2. **Fail soft per item, hard per source.** One unparseable search result is a
   rejection; a source that is entirely unreachable raises. The distinction
   matters because the first is normal and the second is not.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from ..http.client import HttpClient
from ..config import SourceSettings
from ..logging_setup import get_logger
from ..models import RawRecord, Rejection, SourceName

log = get_logger(__name__)

#: Extensions we are willing to accept from a URL. Advisory only — the real
#: format is always determined from the bytes at validation. This just avoids
#: downloading obvious video/vector files from a category listing.
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff")


class ImageSource(ABC):
    """Base class for anything that can produce candidate images for a class."""

    #: Must match a value in the CHECK constraint on images.source.
    name: SourceName

    def __init__(self, client: HttpClient, settings: SourceSettings) -> None:
        self.client = client
        self.settings = settings
        #: Per-item failures collected during fetch. The caller drains these
        #: into the rejections table rather than the source writing to the DB
        #: itself — sources stay free of database concerns.
        self.rejections: list[Rejection] = []

    @abstractmethod
    def fetch(self, class_label: str, limit: int) -> Iterator[RawRecord]:
        """Yield up to `limit` candidate records for `class_label`."""

    def drain_rejections(self) -> list[Rejection]:
        collected, self.rejections = self.rejections, []
        return collected

    @staticmethod
    def looks_like_image_url(url: str) -> bool:
        path = url.split("?", 1)[0].lower()
        return path.endswith(IMAGE_EXTENSIONS)
