"""
Wikimedia Commons — the scraping source.

The brief asks for images scraped from a real web page rather than a clean JSON
API, and Commons is the honest choice: it is a genuine HTML page, it is legal
to scrape, and every file page carries explicit licensing that a dataset needs.
(Commons *does* expose an API. Using it would defeat the purpose of the
exercise, so this adapter deliberately parses the rendered HTML.)

The scrape is two-phase:

    category page  ->  list of File: pages       (1 request per ~200 files)
    file page      ->  full-res URL + licence    (1 request per image)

The second request is unavoidable: a category listing shows thumbnails and file
names, and nothing about licensing. Since our policy is to reject anything whose
licence we cannot establish, we have to open the file page. This costs one
request per candidate — the reason `commons_max_images_per_class` exists.

Scraping is brittle by nature, so every selector below has fallbacks and every
parse failure becomes a `PARSE_ERROR` rejection rather than an exception. A
source that dies because Wikimedia reordered a table is a source that will
embarrass you at 3am.
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from ..logging_setup import get_logger
from ..models import RawRecord, Rejection, RejectionReason, SourceName, Stage
from .base import ImageSource

log = get_logger(__name__)

#: Licence templates on Commons render their short name in a span with this
#: class. Most-specific selector first; the later ones are progressively
#: coarser nets for pages using older or unusual templates.
_LICENCE_SELECTORS = (
    "span.licensetpl_short",
    ".licensetpl_short",
    "#licensetpl_short",
)

#: Page chrome, stripped before anything is read.
#:
#: This is not tidiness. Every MediaWiki page carries a footer reading "Text is
#: available under the Creative Commons Attribution-ShareAlike 4.0 License",
#: with a link to that deed — and it describes *Commons itself*, never the file
#: on the page. Any licence search that is not scoped away from it will happily
#: read the site's licence and attribute it to the photograph.
#:
#: That is not hypothetical. It shipped: four rows of the delivered dataset
#: carried `license=CC0-1.0` next to a `by-sa/4.0` deed URL taken from the
#: footer. The dangerous version is worse — a file under a licence with no CC
#: deed of its own (GFDL, say) had no other creativecommons.org anchor on the
#: page, so the footer became its licence, and it would have been stamped
#: CC-BY-SA-4.0 and shipped as training data. `MISSING_LICENSE` could not fire
#: on any real Commons page, because the footer always answered first.
#:
#: The fixtures missed it because hand-written test HTML has no page chrome.
#: Real pages do, which is the argument for keeping at least one fixture that
#: looks like the thing rather than like its essentials.
_CHROME_SELECTORS = (
    "#footer",
    ".mw-footer",
    "#footer-info",
    ".printfooter",
    "#mw-navigation",
    "#siteNotice",
    "#catlinks",
)

#: Where a file's *own* licensing lives, most specific first. Searching is
#: confined to these; there is deliberately no whole-document fallback, because
#: the whole document is exactly what caused the defect above.
_LICENCE_SCOPES = (
    "#mw-content-text .licensetpl",
    "#mw-content-text table.layouttemplate",
    "#mw-content-text .license",
    # The same containers on pages that do not wrap content in #mw-content-text,
    # which is a skin detail and has changed more than once.
    ".licensetpl",
    "table.layouttemplate",
    ".license",
    "#mw-imagepage-content",
    "#mw-content-text",
    # Last resort: the body, minus the chrome removed above. Reached only by a
    # page with no recognisable content container at all — and still not the
    # whole document, because the footer is already gone by this point.
    "body",
)

#: Public-domain pages often carry no licence *template* at all, only prose.
#: These patterns are the last resort before rejecting the image outright.
_PD_PATTERNS = (
    re.compile(r"\bpublic domain\b", re.I),
    re.compile(r"\bCC0\b"),
    re.compile(r"\bpublicdomain\b", re.I),
)


class WikimediaCommonsSource(ImageSource):
    name = SourceName.WIKIMEDIA_COMMONS

    def fetch(self, class_label: str, limit: int) -> Iterator[RawRecord]:
        category = self.settings.commons_categories.get(class_label)
        if not category:
            log.warning("commons: no category configured", extra={"class": class_label})
            return

        cap = min(limit, self.settings.commons_max_images_per_class)
        file_pages = self._collect_file_pages(category, cap)
        log.info(
            "commons: category listing parsed",
            extra={"class": class_label, "category": category, "file_pages": len(file_pages)},
        )

        yielded = 0
        for file_url in file_pages:
            if yielded >= cap:
                return
            record = self._scrape_file_page(file_url, class_label)
            if record is not None:
                yielded += 1
                yield record

    # -------------------------------------------------------------------------
    #  Phase 1 — category listing
    # -------------------------------------------------------------------------

    def _collect_file_pages(self, category: str, cap: int) -> list[str]:
        """
        Collect File: page URLs for a category, descending into subcategories
        when the category itself does not hold enough files directly.

        **Why the descent exists.** The first version of this scraper walked
        only the requested category and returned zero files for `Category:Cats`.
        The cause was not a broken selector: broad Commons categories like
        Cats, Dogs and Birds are *container* categories. They are made almost
        entirely of subcategories (`Cats by colour`, `Cats by country`, …) and
        carry few or no media files of their own. Scraping only the top level
        is therefore correct code pointed at the wrong page.

        Rather than hardcode a list of narrow categories — which would break
        the moment Commons reorganises — the scraper handles the structure it
        actually meets: take whatever files the category holds, and if that is
        short of the target, walk one level into its subcategories.

        Every step logs its counts, so a future zero is diagnosable from the
        run's own logs instead of requiring a debugging session.
        """
        collected: list[str] = []
        seen: set[str] = set()
        # Over-collect: some file pages fail to parse or carry no licence.
        target = cap * 2

        n_direct = self._harvest_category(
            category, collected, seen, target, depth=0
        )
        log.info(
            "commons: direct files in category",
            extra={"category": category, "files": n_direct},
        )

        if len(collected) < target and self.settings.commons_follow_subcategories:
            subcategories = self._list_subcategories(category)
            log.info(
                "commons: descending into subcategories",
                extra={
                    "category": category,
                    "subcategories_found": len(subcategories),
                    "will_visit": min(len(subcategories),
                                      self.settings.commons_max_subcategories),
                    "collected_so_far": len(collected),
                },
            )
            for subcategory in subcategories[: self.settings.commons_max_subcategories]:
                if len(collected) >= target:
                    break
                self._harvest_category(subcategory, collected, seen, target, depth=1)

        if not collected:
            # Loud, because silence here is what produced a dataset with no
            # scraped images at all and nothing in the logs to explain it.
            log.error(
                "commons: collected ZERO file pages — the scraped source will "
                "contribute nothing. Check the category name and page markup.",
                extra={"category": category},
            )

        log.info(
            "commons: file pages collected",
            extra={"category": category, "total": len(collected), "target": target},
        )
        return collected

    def _harvest_category(
        self, category: str, collected: list[str], seen: set[str], target: int, depth: int
    ) -> int:
        """
        Pull File: links out of one category, following its `filefrom=`
        pagination. Returns how many *new* links this category contributed.
        """
        added = 0
        page_url = urljoin(self.settings.commons_base_url, f"/wiki/{category}")

        for _ in range(self.settings.commons_max_pages):
            if len(collected) >= target:
                break
            soup = self._get_soup(page_url)
            if soup is None:
                break

            for href in self._file_links(soup):
                if href in seen:
                    continue
                seen.add(href)
                collected.append(href)
                added += 1
                if len(collected) >= target:
                    break

            next_link = soup.find(
                "a",
                href=re.compile(r"filefrom="),
                string=re.compile("next page", re.I),
            )
            if not next_link:
                break
            page_url = urljoin(self.settings.commons_base_url, next_link["href"])

        if depth > 0:
            log.debug("commons: subcategory harvested",
                      extra={"category": category, "added": added})
        return added

    def _file_links(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract File: links from a category page, most-specific selector first.

        Commons renders category media in a few different shapes depending on
        skin and page age, so this tries them in order of precision and only
        falls back to a broad sweep — scoped to the content area, and filtered
        to image extensions — when the structured containers yield nothing.
        """
        candidates = []
        for selector in (
            "#mw-category-media li.gallerybox",   # modern category media section
            "li.gallerybox",                      # any gallery on the page
            ".gallerytext",                       # older gallery markup
        ):
            boxes = soup.select(selector)
            if boxes:
                for box in boxes:
                    link = box.select_one('a[href^="/wiki/File:"]')
                    if link and link.get("href"):
                        candidates.append(link["href"])
                if candidates:
                    break

        if not candidates:
            # Last resort. Scoped to #mw-content-text so page furniture and
            # navigation links are excluded; the extension filter below does
            # the rest.
            content = soup.select_one("#mw-content-text") or soup
            candidates = [
                a["href"] for a in content.select('a[href^="/wiki/File:"]') if a.get("href")
            ]

        out: list[str] = []
        for href in candidates:
            absolute = urljoin(self.settings.commons_base_url, href)
            # Category listings mix SVG diagrams, OGV video and PDFs in with
            # photographs. Filtering on the file name here saves one request
            # per non-photo — cheaper for us and politer to Commons.
            if self.looks_like_image_url(unquote(absolute)):
                out.append(absolute)
        return out

    def _list_subcategories(self, category: str) -> list[str]:
        """Subcategory titles listed on a category page, e.g. 'Category:Kittens'."""
        soup = self._get_soup(urljoin(self.settings.commons_base_url, f"/wiki/{category}"))
        if soup is None:
            return []

        container = soup.select_one("#mw-subcategories") or soup.select_one("#mw-content-text")
        if container is None:
            return []

        titles: list[str] = []
        seen: set[str] = set()
        for link in container.select('a[href^="/wiki/Category:"]'):
            href = link.get("href", "")
            title = unquote(href.split("/wiki/", 1)[-1]).split("#")[0]
            if not title.startswith("Category:") or title == category:
                continue
            # Maintenance and metadata categories contain no photographs.
            if any(marker in title for marker in ("by_country", "by_date", "Media_",
                                                  "_by_year", "Categories_")):
                continue
            if title not in seen:
                seen.add(title)
                titles.append(title)
        return titles

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        """Fetch and parse a page, returning None (with a log line) on any failure."""
        if not self.client.may_fetch(url):
            log.warning("commons: robots.txt disallows", extra={"url": url})
            return None
        try:
            response = self.client.get(url)
            response.raise_for_status()
        except requests.RequestException as exc:
            log.error("commons: page fetch failed",
                      extra={"url": url, "error": str(exc)})
            return None
        try:
            return BeautifulSoup(response.text, "lxml")
        except Exception as exc:
            log.error("commons: page parse failed",
                      extra={"url": url, "error": str(exc)})
            return None

    # -------------------------------------------------------------------------
    #  Phase 2 — file page
    # -------------------------------------------------------------------------

    def _scrape_file_page(self, file_url: str, class_label: str) -> RawRecord | None:
        if not self.client.may_fetch(file_url):
            self._reject(class_label, file_url, RejectionReason.PARSE_ERROR,
                         "robots.txt disallows this path")
            return None
        try:
            response = self.client.get(file_url)
            response.raise_for_status()
        except requests.RequestException as exc:
            self._reject(class_label, file_url, RejectionReason.PARSE_ERROR,
                         f"file page fetch failed: {exc}")
            return None

        # Parsing is wrapped because HTML is hostile input: a malformed page, an
        # unexpected encoding or a selector that suddenly matches something odd
        # must produce one rejection, never take down a run that has already
        # collected a hundred good images.
        try:
            soup = self.strip_chrome(BeautifulSoup(response.text, "lxml"))
            image_url = self._extract_full_image_url(soup)
            licence = self._extract_licence(soup)
            licence_url = self._extract_licence_url(soup)
            author = self._extract_author(soup)
        except Exception as exc:
            self._reject(class_label, file_url, RejectionReason.PARSE_ERROR,
                         f"{type(exc).__name__}: {exc}")
            return None

        if not image_url:
            self._reject(class_label, file_url, RejectionReason.MISSING_IMAGE_URL,
                         "could not locate full-resolution image link")
            return None

        if not licence:
            # Strict policy: unlicensed means unusable, so we stop here rather
            # than download bytes we would have to throw away.
            self._reject(class_label, file_url, RejectionReason.MISSING_LICENSE,
                         "no licence template or public-domain marker found")
            return None

        title = unquote(file_url.rsplit("File:", 1)[-1]).replace("_", " ")

        return RawRecord(
            source=self.name,
            class_label=class_label,
            image_url=image_url,
            #: The file page URL is Commons' stable identifier for the asset.
            source_id=file_url,
            landing_url=file_url,
            license_raw=licence,
            license_url=licence_url,
            attribution=author,
            title=title,
            # Only what we parsed, plus the page URL — storing the whole HTML
            # document in JSONB would bloat the raw table for no benefit.
            payload={
                "file_page": file_url,
                "image_url": image_url,
                "license_raw": licence,
                "title": title,
                "scraped_from": "html",
            },
        )

    # -------------------------------------------------------------------------
    #  Selectors, each with fallbacks
    # -------------------------------------------------------------------------

    @staticmethod
    def _extract_full_image_url(soup: BeautifulSoup) -> str | None:
        # Preferred: the "original file" link under the preview.
        link = soup.select_one(".fullImageLink a[href]")
        if link:
            return WikimediaCommonsSource._absolutise(link["href"])
        # Fallback: the preview img itself. Its src is a thumbnail, so strip the
        # /thumb/ path segment to recover the original — a documented Commons
        # URL convention, not a guess.
        img = soup.select_one("#file img[src]") or soup.select_one(".fullMedia img[src]")
        if img:
            src = WikimediaCommonsSource._absolutise(img["src"])
            return re.sub(r"/thumb(/.+?\.\w+)/[^/]+$", r"\1", src)
        return None

    @staticmethod
    def _absolutise(href: str) -> str:
        """Commons emits protocol-relative URLs (//upload.wikimedia.org/...)."""
        return f"https:{href}" if href.startswith("//") else href

    @staticmethod
    def _licence_from_cc_url(href: str) -> str | None:
        """
        Derive a licence code from a Creative Commons deed URL.

        `https://creativecommons.org/licenses/by-sa/3.0/`      -> "by-sa-3.0"
        `https://creativecommons.org/publicdomain/zero/1.0/`   -> "cc0"
        `https://creativecommons.org/publicdomain/mark/1.0/`   -> "public domain"

        This is the most reliable signal on a Commons file page. The rendered
        licence *text* varies by template, by language and by editor; the deed
        URL is machine-generated and stable. Whatever comes back here is fed
        through `normalise_license`, so the mapping to SPDX stays in one place.
        """
        match = re.search(
            r"creativecommons\.org/licenses/([a-z\-]+)(?:/(\d+\.\d+))?", href, re.I
        )
        if match:
            elements, version = match.group(1), match.group(2)
            return f"{elements}-{version}" if version else elements
        if re.search(r"creativecommons\.org/publicdomain/zero", href, re.I):
            return "cc0"
        if re.search(r"creativecommons\.org/publicdomain/mark", href, re.I):
            return "public domain"
        return None

    @staticmethod
    def strip_chrome(soup: BeautifulSoup) -> BeautifulSoup:
        """
        Remove the site's own furniture before reading anything from the page.

        Called once, immediately after parsing, so that every extractor below
        sees only the file's own content. See `_CHROME_SELECTORS` for why this
        is a correctness measure and not housekeeping.
        """
        for selector in _CHROME_SELECTORS:
            for node in soup.select(selector):
                node.decompose()
        return soup

    @staticmethod
    def _licence_scope(soup: BeautifulSoup):
        """
        The narrowest container that plausibly holds this file's licensing.

        Strips chrome first rather than assuming a caller already did. The
        guarantee "the footer can never be read as a licence" should hold for
        anyone who calls these functions — including a future test that builds
        a soup by hand — and not depend on remembering to call something first.
        `decompose()` on an already-removed node is a no-op, so paying for this
        twice costs nothing.
        """
        WikimediaCommonsSource.strip_chrome(soup)
        for selector in _LICENCE_SCOPES:
            node = soup.select_one(selector)
            if node is not None:
                return node
        return None

    @staticmethod
    def _extract_licence(soup: BeautifulSoup) -> str | None:
        """
        Establish the licence, trying the most trustworthy signal first.

        Order matters and is deliberate:
          1. the deed URL — machine-generated, language-independent, stable
          2. the `licensetpl_short` span — the template's own machine-readable
             short name, present on most but not all file pages
          3. public-domain prose — a genuine last resort, and scoped tightly

        Ordering the URL first is a change from the original implementation.
        Reading the rendered text first meant a page whose template rendered in
        German, or with an unusual short name, was rejected as unlicensed even
        though its deed link said exactly what the licence was.

        Every step is confined to the file's own licensing area. A page whose
        licence we cannot find there is rejected as unlicensed — which is the
        correct answer, and the one the footer used to prevent us from giving.
        """
        scope = WikimediaCommonsSource._licence_scope(soup)
        if scope is None:
            return None

        for anchor in scope.select('a[href*="creativecommons.org"]'):
            derived = WikimediaCommonsSource._licence_from_cc_url(anchor.get("href", ""))
            if derived:
                return derived

        for selector in _LICENCE_SELECTORS:
            node = scope.select_one(selector)
            if node and node.get_text(strip=True):
                return node.get_text(strip=True)

        # Public-domain prose. Scoped to the licensing containers only, never
        # the whole page: the words "public domain" appear in unrelated
        # navigation and boilerplate and would produce false positives.
        for selector in (
            "#mw-content-text .licensetpl",
            "table.layouttemplate",
            ".licensetpl",
            "#mw-imagepage-content .license",
        ):
            section = soup.select_one(selector)
            if section is None:
                continue
            haystack = section.get_text(" ", strip=True)
            for pattern in _PD_PATTERNS:
                if pattern.search(haystack):
                    return "Public domain"
        return None

    @staticmethod
    def _extract_licence_url(soup: BeautifulSoup) -> str | None:
        """
        The deed URL for *this file*, in document order within its own scope.

        The previous version preferred `/licenses` over `/publicdomain` across
        the whole page. On a CC0 file that is precisely backwards: the file's
        own deed is `/publicdomain/zero/1.0/`, the only `/licenses` link is the
        site footer's, and the preference reached past the right answer to pick
        the wrong one. Four rows of the delivered dataset say `CC0-1.0` beside a
        `by-sa/4.0` URL for exactly that reason.
        """
        scope = WikimediaCommonsSource._licence_scope(soup)
        if scope is None:
            return None
        link = scope.select_one('a[href*="creativecommons.org"]')
        return WikimediaCommonsSource._absolutise(link["href"]) if link else None

    @staticmethod
    def _extract_author(soup: BeautifulSoup) -> str | None:
        """
        Author lives in the file-info table, whose markup has changed over the
        years. Try the modern id, then the legacy class, then give up — a
        missing author is not fatal, since the licence is what we must retain.
        """
        node = soup.select_one("#fileinfotpl_aut ~ td") or soup.select_one("td.fileinfo-paramfield + td")
        if node:
            text = node.get_text(" ", strip=True)
            return text[:500] if text else None
        return None

    # -------------------------------------------------------------------------

    def _reject(
        self, class_label: str, url: str, reason: RejectionReason, detail: str
    ) -> None:
        self.rejections.append(
            Rejection(
                stage=Stage.INGEST,
                reason_code=reason,
                source=self.name,
                class_label=class_label,
                source_id=url,
                source_url=url,
                detail=detail,
            )
        )
