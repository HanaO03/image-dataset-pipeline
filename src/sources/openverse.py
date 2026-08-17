"""
Openverse — the API source.

Chosen over the alternatives for one reason that matters to a *training*
dataset: Openverse returns `license`, `license_version`, `license_url` and a
ready-made `attribution` string on every result. Licensing is not an
afterthought we have to reconstruct; it arrives as structured data. Since our
policy rejects any image whose licence we cannot determine, a source that
volunteers it is worth a great deal.

It is also no-auth by default. Anonymous requests are rate limited, so this
adapter additionally supports the free OAuth client credentials Openverse hands
out — set OPENVERSE_CLIENT_ID / OPENVERSE_CLIENT_SECRET and the adapter
authenticates automatically. Without them it still works, just more slowly.
"""

from __future__ import annotations

from typing import Any, Iterator

import requests

from ..config import SourceSettings
from ..http.client import HttpClient
from ..logging_setup import get_logger
from ..models import RawRecord, Rejection, RejectionReason, SourceName, Stage
from .base import ImageSource

log = get_logger(__name__)


class OpenverseSource(ImageSource):
    name = SourceName.OPENVERSE

    def __init__(self, client: HttpClient, settings: SourceSettings) -> None:
        super().__init__(client, settings)
        self._token: str | None = None
        self._authenticate_if_possible()

    # -------------------------------------------------------------------------
    #  Optional authentication
    # -------------------------------------------------------------------------

    def _authenticate_if_possible(self) -> None:
        """
        Exchange client credentials for a bearer token, if credentials exist.

        Deliberately best-effort: an auth failure downgrades us to anonymous
        rather than killing the run. Rate limits are a performance problem;
        refusing to run at all is a correctness problem.
        """
        client_id = self.settings.openverse_client_id
        client_secret = self.settings.openverse_client_secret
        if not client_id or not client_secret:
            log.info("openverse: running anonymously (no credentials configured)")
            return
        try:
            response = self.client.request(
                "POST",
                f"{self.settings.openverse_base_url}/auth_tokens/token/",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret.get_secret_value(),
                    "grant_type": "client_credentials",
                },
            )
            response.raise_for_status()
            self._token = response.json()["access_token"]
            log.info("openverse: authenticated, higher rate limit in effect")
        except Exception as exc:
            log.warning("openverse: auth failed, falling back to anonymous",
                        extra={"error": str(exc)})

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    # -------------------------------------------------------------------------
    #  Fetch
    # -------------------------------------------------------------------------

    def fetch(self, class_label: str, limit: int) -> Iterator[RawRecord]:
        url = f"{self.settings.openverse_base_url}/images/"
        page = 1
        yielded = 0

        while yielded < limit:
            params = {
                "q": class_label,
                "page": page,
                "page_size": self.settings.openverse_page_size,
                # Restrict upstream to reusable licences. Filtering at the
                # source is cheaper and more reliable than downloading first
                # and discarding later.
                "license_type": self.settings.openverse_license_type,
                "category": "photograph",
                "mature": "false",
            }
            try:
                payload = self.client.get_json(url, params=params, headers=self._headers)
            except requests.RequestException as exc:
                # A dead source is infrastructure, not data. Stop this source
                # cleanly and let the run continue with whatever it collected;
                # the quality gate will flag the shortfall.
                log.error("openverse: search failed, stopping this source",
                          extra={"class": class_label, "page": page, "error": str(exc)})
                return

            results = payload.get("results") or []
            if not results:
                log.info("openverse: no more results",
                         extra={"class": class_label, "page": page, "yielded": yielded})
                return

            for item in results:
                if yielded >= limit:
                    return
                record = self._to_record(item, class_label)
                if record is not None:
                    yielded += 1
                    yield record

            page_count = payload.get("page_count") or 0
            if page >= page_count:
                return
            page += 1

    # -------------------------------------------------------------------------
    #  Parsing
    # -------------------------------------------------------------------------

    def _to_record(self, item: dict[str, Any], class_label: str) -> RawRecord | None:
        """
        Turn one search result into a RawRecord, or record why we could not.

        Every field is read defensively. Openverse aggregates many providers and
        the guarantees differ per provider: `url` is occasionally null, `license`
        is occasionally missing, and `creator` frequently is. Assuming otherwise
        produces a KeyError three hours into a run.
        """
        source_id = str(item.get("id") or "")
        image_url = item.get("url")

        if not image_url:
            self.rejections.append(
                Rejection(
                    stage=Stage.INGEST,
                    reason_code=RejectionReason.MISSING_IMAGE_URL,
                    source=self.name,
                    class_label=class_label,
                    source_id=source_id or None,
                    source_url=item.get("foreign_landing_url"),
                    detail="result had no direct image url",
                )
            )
            return None

        # Strict licence policy, applied at the earliest possible point: no
        # point spending a download on bytes we are contractually unable to use.
        if not item.get("license"):
            self.rejections.append(
                Rejection(
                    stage=Stage.INGEST,
                    reason_code=RejectionReason.MISSING_LICENSE,
                    source=self.name,
                    class_label=class_label,
                    source_id=source_id or None,
                    source_url=item.get("foreign_landing_url"),
                    detail="no license field on result",
                )
            )
            return None

        # Raw licence string kept in the form the source uses ("by" + "4.0");
        # translation to SPDX happens in the normalise stage, so the raw value
        # survives in raw_records and remains auditable.
        version = item.get("license_version")
        license_raw = f"{item['license']}-{version}" if version else str(item["license"])

        return RawRecord(
            source=self.name,
            class_label=class_label,
            image_url=image_url,
            source_id=source_id or image_url,
            landing_url=item.get("foreign_landing_url"),
            license_raw=license_raw,
            license_url=item.get("license_url"),
            # Openverse builds the attribution string for us, which is exactly
            # what CC licences require us to reproduce. Use it verbatim.
            attribution=item.get("attribution") or item.get("creator"),
            title=item.get("title"),
            payload=item,
        )
