"""
Source adapter tests, run entirely against canned payloads.

No network. The fixtures below are trimmed copies of what Openverse and Commons
actually return, including the parts that are missing or malformed — because
the interesting behaviour of a source adapter is what it does with the bad
records, not the good ones.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from src.config import SourceSettings
from src.models import RejectionReason, SourceName
from src.sources.openverse import OpenverseSource
from src.sources.wikimedia import WikimediaCommonsSource


# =============================================================================
#  Test doubles
# =============================================================================


class FakeResponse:
    def __init__(self, text: str = "", status: int = 200) -> None:
        self.text = text
        self.status_code = status

    def raise_for_status(self) -> None:
        # Must raise the same exception type `requests` does, or the test
        # double silently exercises a different code path than production.
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeClient:
    """Records the URLs requested, so politeness can be asserted too."""

    def __init__(self, json_pages: list[dict] | None = None,
                 pages: dict[str, str] | None = None) -> None:
        self._json_pages = json_pages or []
        self._pages = pages or {}
        self.requested: list[str] = []

    def get_json(self, url: str, **kwargs: Any) -> dict:
        self.requested.append(url)
        return self._json_pages.pop(0) if self._json_pages else {"results": []}

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested.append(url)
        if url not in self._pages:
            return FakeResponse("", status=404)
        return FakeResponse(self._pages[url])

    def may_fetch(self, url: str) -> bool:
        return True

    def request(self, *args: Any, **kwargs: Any):
        raise AssertionError("no source should issue a raw request in tests")


@pytest.fixture
def source_settings() -> SourceSettings:
    return SourceSettings()


# =============================================================================
#  Openverse
# =============================================================================

OPENVERSE_PAGE = {
    "result_count": 4,
    "page_count": 1,
    "results": [
        {   # well-formed
            "id": "ov-0001",
            "title": "Sleeping cat",
            "url": "https://live.staticflickr.com/1/cat.jpg",
            "foreign_landing_url": "https://flickr.com/photos/x/1",
            "creator": "Jane Doe",
            "license": "by",
            "license_version": "2.0",
            "license_url": "https://creativecommons.org/licenses/by/2.0/",
            "attribution": '"Sleeping cat" by Jane Doe is licensed under CC BY 2.0',
        },
        {   # no direct image url — happens on some providers
            "id": "ov-0002",
            "title": "Broken record",
            "url": None,
            "foreign_landing_url": "https://example.org/2",
            "license": "by-sa",
            "license_version": "4.0",
        },
        {   # no licence at all — rejected by the strict policy before download
            "id": "ov-0003",
            "title": "Unlicensed",
            "url": "https://live.staticflickr.com/3/cat.jpg",
            "license": None,
        },
        {   # licence with no version, and no creator
            "id": "ov-0004",
            "title": "Public domain cat",
            "url": "https://live.staticflickr.com/4/cat.jpg",
            "license": "cc0",
            "license_version": None,
        },
    ],
}


def test_openverse_parses_good_records_and_rejects_the_rest(source_settings):
    client = FakeClient(json_pages=[OPENVERSE_PAGE])
    source = OpenverseSource(client, source_settings)

    records = list(source.fetch("cat", limit=10))

    assert [r.source_id for r in records] == ["ov-0001", "ov-0004"]
    assert all(r.source is SourceName.OPENVERSE for r in records)
    assert all(r.class_label == "cat" for r in records)

    reasons = {r.reason_code for r in source.drain_rejections()}
    assert reasons == {
        RejectionReason.MISSING_IMAGE_URL,
        RejectionReason.MISSING_LICENSE,
    }


def test_openverse_keeps_the_raw_payload_for_replay(source_settings):
    """
    The whole upstream object is retained so normalisation can be re-run later
    without paying the rate limit again.
    """
    client = FakeClient(json_pages=[OPENVERSE_PAGE])
    record = next(iter(OpenverseSource(client, source_settings).fetch("cat", 1)))
    assert record.payload["title"] == "Sleeping cat"
    assert record.payload["id"] == "ov-0001"


def test_openverse_combines_licence_and_version_without_losing_either(source_settings):
    client = FakeClient(json_pages=[OPENVERSE_PAGE])
    records = list(OpenverseSource(client, source_settings).fetch("cat", 10))
    assert records[0].license_raw == "by-2.0"
    assert records[1].license_raw == "cc0", "a versionless licence must not gain a stray dash"


def test_openverse_stops_at_the_requested_limit(source_settings):
    client = FakeClient(json_pages=[OPENVERSE_PAGE])
    assert len(list(OpenverseSource(client, source_settings).fetch("cat", 1))) == 1


def test_openverse_survives_a_dead_api(source_settings):
    """
    An unreachable source must degrade to zero records, not an exception. The
    quality gate reports the shortfall; the run still produces what it can from
    the other source.
    """
    import requests

    class DeadClient(FakeClient):
        def get_json(self, url: str, **kwargs: Any) -> dict:
            raise requests.RequestException("connection reset")

    records = list(OpenverseSource(DeadClient(), source_settings).fetch("cat", 10))
    assert records == []


# =============================================================================
#  Wikimedia Commons (scraping)
# =============================================================================

CATEGORY_HTML = """
<html><body><div id="mw-content-text">
  <ul class="gallery">
    <li class="gallerybox"><div class="thumb">
      <a href="/wiki/File:Cat_one.jpg" class="image"><img src="//upload.wikimedia.org/x.jpg"></a>
    </div><div class="gallerytext">
      <a href="/wiki/File:Cat_one.jpg" title="File:Cat one.jpg">Cat one.jpg</a>
    </div></li>
    <li class="gallerybox"><div class="thumb">
      <a href="/wiki/File:Cat_two.jpg" class="image"><img src="//upload.wikimedia.org/y.jpg"></a>
    </div></li>
    <li class="gallerybox"><div class="thumb">
      <a href="/wiki/File:Cat_three.jpg" class="image"><img src="//upload.wikimedia.org/z.jpg"></a>
    </div></li>
    <li class="gallerybox"><div class="thumb">
      <a href="/wiki/File:Cat_diagram.svg" class="image"><img src="//upload.wikimedia.org/d.png"></a>
    </div></li>
  </ul>
</div></body></html>
"""


#: Every real MediaWiki page ends with this. It states the licence of *Commons*,
#: not of the file, and it is the reason four rows of the delivered dataset
#: carried a licence URL belonging to the website rather than the photograph.
#: Every fixture below now carries it, because a fixture that omits the page
#: furniture cannot catch a bug caused by the page furniture.
MW_FOOTER = """
  <div id="catlinks"><a href="/wiki/Category:Cats">Cats</a></div>
  <div id="footer" class="mw-footer"><ul id="footer-info">
    <li>Text is available under the
      <a rel="license" href="https://creativecommons.org/licenses/by-sa/4.0/">
      Creative Commons Attribution-ShareAlike 4.0 License</a>.</li>
  </ul></div>
"""

FILE_PAGE_CC = f"""
<html><body>
  <div class="fullImageLink" id="file">
    <a href="//upload.wikimedia.org/wikipedia/commons/1/15/Cat_one.jpg">
      <img src="//upload.wikimedia.org/wikipedia/commons/thumb/1/15/Cat_one.jpg/800px-Cat_one.jpg">
    </a>
  </div>
  <table class="fileinfotpl-type-information">
    <tr><td id="fileinfotpl_aut" class="fileinfo-paramfield">Author</td><td>Alvesgaspar</td></tr>
  </table>
  <div id="mw-content-text"><div class="licensetpl">
    <span class="licensetpl_short">CC BY-SA 3.0</span>
    <a href="https://creativecommons.org/licenses/by-sa/3.0/">link</a>
  </div></div>
  {MW_FOOTER}
</body></html>
"""

FILE_PAGE_PD = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/2/22/Cat_two.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text"><table class="layouttemplate">
    <tr><td>This work is in the <b>public domain</b> in its country of origin.</td></tr>
  </table></div>
  {MW_FOOTER}
</body></html>
"""

FILE_PAGE_NO_LICENCE = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/3/33/Cat_three.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text"><p>Someone forgot to add a licence template.</p></div>
  {MW_FOOTER}
</body></html>
"""


#: A file under a licence with no Creative Commons deed of its own. The only
#: creativecommons.org anchor on the page is the site footer's — which is
#: exactly how it used to be stamped CC-BY-SA-4.0 and shipped as training data.
FILE_PAGE_GFDL = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/4/44/Cat_four.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text"><table class="layouttemplate licensetpl">
    <tr><td>Permission is granted to copy, distribute and/or modify this document
    under the terms of the <b>GNU Free Documentation License</b>, Version 1.2.</td></tr>
  </table></div>
  {MW_FOOTER}
</body></html>
"""


#: A CC0 file. Its own deed lives under /publicdomain/zero/; the only
#: /licenses/ link on the page belongs to the footer. Preferring "/licenses"
#: over "/publicdomain" across the whole document is what put a by-sa/4.0 URL
#: next to `license=CC0-1.0` in the delivered dataset.
FILE_PAGE_CC0 = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/5/55/Cat_five.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text"><div class="licensetpl">
    <span class="licensetpl_short">CC0</span>
    <a href="https://creativecommons.org/publicdomain/zero/1.0/deed.en">CC0</a>
  </div></div>
  {MW_FOOTER}
</body></html>
"""

BASE = "https://commons.wikimedia.org"


def _commons_client() -> FakeClient:
    return FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_CC,
            f"{BASE}/wiki/File:Cat_two.jpg": FILE_PAGE_PD,
            f"{BASE}/wiki/File:Cat_three.jpg": FILE_PAGE_NO_LICENCE,
        }
    )


def test_commons_scrapes_licensed_images_and_rejects_unlicensed(source_settings):
    source = WikimediaCommonsSource(_commons_client(), source_settings)
    records = list(source.fetch("cat", limit=10))

    assert len(records) == 2, "the unlicensed file must not become a record"
    assert records[0].image_url == (
        "https://upload.wikimedia.org/wikipedia/commons/1/15/Cat_one.jpg"
    )
    # The deed URL is preferred over the rendered short name, so the raw value
    # is the machine-derived code. What matters downstream is that it normalises
    # to the right SPDX identifier — assert that, not the intermediate spelling.
    from src.pipeline.normalize import normalise_license

    assert records[0].license_raw == "by-sa-3.0"
    assert normalise_license(records[0].license_raw) == "CC-BY-SA-3.0"
    assert records[0].license_url == "https://creativecommons.org/licenses/by-sa/3.0/"
    assert records[0].attribution == "Alvesgaspar"

    # Public domain expressed as prose, with no licence template at all.
    assert records[1].license_raw == "Public domain"

    rejections = source.drain_rejections()
    assert [r.reason_code for r in rejections] == [RejectionReason.MISSING_LICENSE]


def test_commons_skips_non_photo_file_types_before_spending_a_request(source_settings):
    """
    Category listings mix SVG diagrams, videos and PDFs in with photographs.
    Filtering on the file name in the listing avoids opening a page for a file
    we would reject anyway — one saved request per non-photo, and it is polite.
    """
    client = _commons_client()
    list(WikimediaCommonsSource(client, source_settings).fetch("cat", 10))
    assert not any("Cat_diagram.svg" in url for url in client.requested)


def test_commons_protocol_relative_urls_are_made_absolute(source_settings):
    records = list(
        WikimediaCommonsSource(_commons_client(), source_settings).fetch("cat", 1)
    )
    assert records[0].image_url.startswith("https://")


def test_commons_falls_back_to_de_thumbnailing_when_the_original_link_is_absent(
    source_settings,
):
    """
    Older file pages omit `.fullImageLink`. The fallback recovers the original
    by stripping the `/thumb/.../NNNpx-` segment — a documented Commons URL
    convention, not a guess.
    """
    html = """
    <html><body>
      <div id="file"><img src="//upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Cat_one.jpg/640px-Cat_one.jpg"></div>
      <span class="licensetpl_short">CC BY 4.0</span>
    </body></html>
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": html,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 1))
    assert records[0].image_url == (
        "https://upload.wikimedia.org/wikipedia/commons/a/ab/Cat_one.jpg"
    )


def test_commons_records_a_rejection_when_a_file_page_is_unreachable(source_settings):
    """One dead page is a rejection, not a crash."""
    client = FakeClient(pages={f"{BASE}/wiki/Category:Cats": CATEGORY_HTML})
    source = WikimediaCommonsSource(client, source_settings)
    records = list(source.fetch("cat", limit=10))

    assert records == []
    assert all(
        r.reason_code is RejectionReason.PARSE_ERROR for r in source.drain_rejections()
    )


def test_commons_respects_its_own_per_class_cap(source_settings):
    source_settings.commons_max_images_per_class = 1
    records = list(
        WikimediaCommonsSource(_commons_client(), source_settings).fetch("cat", 100)
    )
    assert len(records) == 1


def test_commons_handles_a_category_with_no_gallery(source_settings):
    client = FakeClient(pages={f"{BASE}/wiki/Category:Cats": "<html><body>empty</body></html>"})
    assert list(WikimediaCommonsSource(client, source_settings).fetch("cat", 10)) == []


# =============================================================================
#  Regression tests for the zero-scraped-images failure
# =============================================================================

CONTAINER_CATEGORY_HTML = """
<html><body><div id="mw-content-text">
  <div id="mw-subcategories">
    <h3>Subcategories</h3>
    <ul>
      <li><a href="/wiki/Category:Kittens">Kittens</a></li>
      <li><a href="/wiki/Category:Cats_by_country">Cats by country</a></li>
      <li><a href="/wiki/Category:Sleeping_cats">Sleeping cats</a></li>
    </ul>
  </div>
  <p>This category has no media files of its own.</p>
</div></body></html>
"""

SUBCATEGORY_HTML = """
<html><body><div id="mw-content-text">
  <div id="mw-category-media">
    <ul class="gallery">
      <li class="gallerybox"><div class="thumb">
        <a href="/wiki/File:Cat_one.jpg" class="image"><img src="//upload.wikimedia.org/x.jpg"></a>
      </div></li>
      <li class="gallerybox"><div class="thumb">
        <a href="/wiki/File:Cat_two.jpg" class="image"><img src="//upload.wikimedia.org/y.jpg"></a>
      </div></li>
    </ul>
  </div>
</div></body></html>
"""


def test_container_category_with_no_files_descends_into_subcategories(source_settings):
    """
    The exact failure seen on the first real run.

    `Category:Cats` on Commons is a container: almost entirely subcategories,
    with no media of its own. Scraping only the top level returned zero images
    and the dataset shipped with nothing from the scraped source.
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CONTAINER_CATEGORY_HTML,
            f"{BASE}/wiki/Category:Kittens": SUBCATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_CC,
            f"{BASE}/wiki/File:Cat_two.jpg": FILE_PAGE_PD,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", limit=10))

    assert len(records) == 2, "must recover files from the subcategory"
    assert any("Category:Kittens" in url for url in client.requested)


def test_metadata_subcategories_are_skipped(source_settings):
    """`Cats by country` is an index of other categories, not a photo album."""
    client = FakeClient(pages={f"{BASE}/wiki/Category:Cats": CONTAINER_CATEGORY_HTML})
    list(WikimediaCommonsSource(client, source_settings).fetch("cat", limit=10))
    assert not any("by_country" in url for url in client.requested)


def test_licence_is_read_from_the_deed_url_when_the_template_text_is_absent(
    source_settings,
):
    """
    The deed URL is machine-generated and language-independent; the rendered
    short name is neither. A page with a CC link but no `licensetpl_short`
    must still be usable.
    """
    html = """
    <html><body>
      <div class="fullImageLink"><a href="//upload.wikimedia.org/a/ab/Cat_one.jpg"><img src="x"></a></div>
      <div class="licensetpl">
        <a href="https://creativecommons.org/licenses/by-nc-sa/2.5/">Lizenz</a>
      </div>
    </body></html>
    """
    client = FakeClient(
        pages={f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
               f"{BASE}/wiki/File:Cat_one.jpg": html}
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 1))
    assert records[0].license_raw == "by-nc-sa-2.5"


def test_deed_url_parsing_covers_the_public_domain_forms():
    from src.sources.wikimedia import WikimediaCommonsSource as W

    assert W._licence_from_cc_url("https://creativecommons.org/licenses/by-sa/3.0/") == "by-sa-3.0"
    assert W._licence_from_cc_url("https://creativecommons.org/licenses/by/4.0/") == "by-4.0"
    assert W._licence_from_cc_url("https://creativecommons.org/publicdomain/zero/1.0/") == "cc0"
    assert W._licence_from_cc_url("https://creativecommons.org/publicdomain/mark/1.0/") == "public domain"
    assert W._licence_from_cc_url("https://example.org/nothing") is None


def test_derived_licences_normalise_all_the_way_to_spdx():
    """
    The scraper's output must survive the normalise stage — otherwise images
    are collected only to be rejected later as UNRECOGNISED_LICENSE.
    """
    from src.pipeline.normalize import normalise_license
    from src.sources.wikimedia import WikimediaCommonsSource as W

    for url, expected in [
        ("https://creativecommons.org/licenses/by-sa/3.0/", "CC-BY-SA-3.0"),
        ("https://creativecommons.org/licenses/by/2.0/", "CC-BY-2.0"),
        ("https://creativecommons.org/licenses/by-nc-nd/4.0/", "CC-BY-NC-ND-4.0"),
        ("https://creativecommons.org/publicdomain/zero/1.0/", "CC0-1.0"),
        ("https://creativecommons.org/publicdomain/mark/1.0/", "PDM-1.0"),
    ]:
        assert normalise_license(W._licence_from_cc_url(url)) == expected


def test_broad_sweep_fallback_when_gallery_markup_is_absent(source_settings):
    """
    Some category pages render media as a plain list. The fallback sweep is
    scoped to the content area and filtered by extension, so it picks up the
    photographs without dragging in navigation.
    """
    html = """
    <html><body>
      <div id="mw-navigation"><a href="/wiki/File:Logo.png">nav logo</a></div>
      <div id="mw-content-text">
        <a href="/wiki/File:Cat_one.jpg">Cat one</a>
        <a href="/wiki/File:Cat_diagram.svg">a diagram</a>
        <a href="/wiki/Help:Contents">help</a>
      </div>
    </body></html>
    """
    client = FakeClient(
        pages={f"{BASE}/wiki/Category:Cats": html,
               f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_CC}
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 10))

    assert len(records) == 1
    assert not any("Logo.png" in url for url in client.requested), "nav must be excluded"
    assert not any("diagram.svg" in url for url in client.requested), "svg must be filtered"


# =============================================================================
#  Page chrome must never be read as the file's licence
# =============================================================================
#
#  Every fixture above now ends with MW_FOOTER, because these three defects all
#  had the same shape and none of them were reachable with chrome-free HTML:
#
#    * a CC0 file shipped with the footer's by-sa/4.0 URL beside it — four rows
#      of the delivered dataset;
#    * a file with no CC deed of its own inherited the footer's licence
#      wholesale, so MISSING_LICENSE could not fire on any real Commons page;
#    * the strict licence policy therefore had a hole precisely where it was
#      most confidently claimed.


def test_a_file_with_no_cc_deed_is_rejected_rather_than_given_the_sites(source_settings):
    """
    GFDL is a real licence, and it is not one this dataset accepts. What must
    never happen is the pipeline reading the footer, deciding the photograph is
    CC-BY-SA-4.0, and shipping it as training data under a licence its author
    never granted.
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_GFDL,
        }
    )
    source = WikimediaCommonsSource(client, source_settings)
    records = list(source.fetch("cat", 5))

    assert records == [], "a page whose only CC link is the footer has no licence"
    reasons = [r.reason_code for r in source.drain_rejections()]
    # The other file pages in the category listing are not served here, so they
    # fail to fetch; what matters is that the GFDL page was rejected for the
    # right reason rather than accepted for the wrong one.
    assert RejectionReason.MISSING_LICENSE in reasons


def test_a_cc0_file_keeps_its_own_deed_url(source_settings):
    """
    The licence and the URL must describe the same thing. They did not: the
    licence came from the file's own CC0 template and the URL from the site
    footer, and the two disagreed in the shipped dataset.csv.
    """
    from src.pipeline.normalize import normalise_license

    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_CC0,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 5))

    assert len(records) == 1
    assert normalise_license(records[0].license_raw) == "CC0-1.0"
    assert "publicdomain/zero" in records[0].license_url
    assert "by-sa" not in records[0].license_url


def test_the_licence_and_its_url_agree_on_every_scraped_record(source_settings):
    """
    The invariant, asserted over every fixture at once rather than one page at
    a time — the check that would have caught this in the delivered CSV.
    """
    from src.pipeline.normalize import normalise_license

    client = _commons_client()
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 10))
    assert records, "fixtures must yield something for this to mean anything"

    for record in records:
        spdx = normalise_license(record.license_raw)
        if not record.license_url or spdx is None:
            continue
        url = record.license_url.lower()
        if spdx.startswith("CC0"):
            assert "publicdomain/zero" in url, (spdx, url)
        elif spdx.startswith("PDM"):
            assert "publicdomain" in url, (spdx, url)
        else:
            elements = spdx[len("CC-"):].rsplit("-", 1)[0].lower()
            assert f"/licenses/{elements}/" in url, (spdx, url)


# =============================================================================
#  Attribution: a credit line, not whatever the first table row said
# =============================================================================
#
#  Both cases below are taken from rows that shipped in the delivered dataset.


FILE_PAGE_DESCRIPTION_FIRST = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/6/66/Cat_six.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text">
    <table class="fileinfotpl-type-information">
      <tr><td class="fileinfo-paramfield">Description</td>
          <td>Southern_cassowary.jpg : Frank Wouters of Antwerp, Belgium
              Head_Peacock.jpg : http://commons.wikimedia.org/wiki/User:Jkather
              Kiwi_hg.jpg : Hannes Grobe and a great many more words besides,
              enough to run past every sensible limit for a credit line</td></tr>
      <tr><td class="fileinfo-paramfield">Date</td><td>2011</td></tr>
      <tr><td class="fileinfo-paramfield">Author</td><td>Snowmanradio</td></tr>
    </table>
    <div class="licensetpl">
      <span class="licensetpl_short">CC BY-SA 3.0</span>
      <a href="https://creativecommons.org/licenses/by-sa/3.0/">deed</a>
    </div>
  </div>
  {MW_FOOTER}
</body></html>
"""

FILE_PAGE_CREATOR_TEMPLATE = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/7/77/Cat_seven.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text">
    <table class="fileinfotpl-type-information">
      <tr><td class="fileinfo-paramfield">Author</td>
          <td>Geoff Charles (1909–2002) Description Welsh photographer and
              photojournalist Date of birth/death 28 January 1909 7 March 2002
              Location of birth Brymbo Authority file : Q5534081
              VIAF : 66195543 ISNI : 0000000044619391 QS:P170,Q5534081</td></tr>
    </table>
    <div class="licensetpl">
      <span class="licensetpl_short">CC0</span>
      <a href="https://creativecommons.org/publicdomain/zero/1.0/">deed</a>
    </div>
  </div>
  {MW_FOOTER}
</body></html>
"""


def test_the_author_row_is_read_not_the_first_row(source_settings):
    """
    `td.fileinfo-paramfield + td` takes the cell after the *first* label in the
    table, and on a Commons information table that label is "Description". A
    montage's component list was credited as its author, truncated mid-word at
    500 characters, and shipped.
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_DESCRIPTION_FIRST,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 5))

    assert len(records) == 1
    assert records[0].attribution == "Snowmanradio"


def test_a_creator_template_is_reduced_to_a_credit(source_settings):
    """
    `{{Creator}}` renders a whole biography: dates, nationality, and a VIAF /
    ISNI / QS authority record. All of that is true and none of it belongs in
    an attribution line.
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_CREATOR_TEMPLATE,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 5))

    attribution = records[0].attribution
    assert attribution.startswith("Geoff Charles")
    for marker in ("VIAF", "ISNI", "QS:P", "Authority file"):
        assert marker not in attribution
    assert len(attribution) <= 200


def test_no_scraped_attribution_is_truncated_mid_word(source_settings):
    """Truncation happens on a word boundary, with an ellipsis to show it did."""
    from src.sources.wikimedia import WikimediaCommonsSource as W

    long_text = " ".join(["Photographer"] * 60)
    cleaned = W._clean_author(long_text)
    assert len(cleaned) <= 201
    assert cleaned.endswith("…")
    assert not cleaned[:-1].rstrip().endswith("Photograph")


def test_an_empty_author_cell_yields_none_rather_than_whitespace(source_settings):
    from src.sources.wikimedia import WikimediaCommonsSource as W

    assert W._clean_author("   ") is None
    assert W._clean_author("VIAF : 66195543") is None


FILE_PAGE_MONTAGE = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/8/88/Cat_eight.png"><img src="x"></a>
  </div>
  <div id="mw-content-text">
    <table class="fileinfotpl-type-information">
      <tr><td class="fileinfo-paramfield">Author</td>
          <td>Southern_cassowary.jpg : Frank Wouters of Antwerp, Belgium
              Head_Peacock.jpg : http://commons.wikimedia.org/wiki/User:Jkather
              Kiwi_hg.jpg : Hannes Grobe Opisthocomus_hoazin.jpg : Kate from UK
              Toco_toucan.jpg : Bernard DUPONT from FRANCE</td></tr>
    </table>
    <div class="licensetpl">
      <span class="licensetpl_short">CC BY-SA 3.0</span>
      <a href="https://creativecommons.org/licenses/by-sa/3.0/">deed</a>
    </div>
  </div>
  {MW_FOOTER}
</body></html>
"""


def test_a_file_with_several_authors_keeps_all_of_them(source_settings):
    """
    A montage's author field is a list of photographers, and every one of them
    is owed credit. Reading the page faithfully is correct here — the defect
    with such a file is that it was collected at all, since it is not a
    photograph of one subject, and that is a content question no cheap check
    answers.

    What this pins is that the *cleaning* does not make things worse: the list
    survives, capped and cut on a word boundary rather than mid-word, which is
    what the previous 500-character truncation did.
    """
    client = FakeClient(
        pages={
            f"{BASE}/wiki/Category:Cats": CATEGORY_HTML,
            f"{BASE}/wiki/File:Cat_one.jpg": FILE_PAGE_MONTAGE,
        }
    )
    records = list(WikimediaCommonsSource(client, source_settings).fetch("cat", 5))

    attribution = records[0].attribution
    assert attribution.startswith("Southern_cassowary.jpg : Frank Wouters")
    assert len(attribution) <= 201
    assert attribution.endswith("…"), "a truncated credit must say that it was truncated"
    assert not attribution[:-1].endswith(" "), "cut on a word, not on a space"


# =============================================================================
#  Two licence boxes on one page
#
#  Commons file pages routinely carry more than one: a public-domain tag for
#  the depicted work beside the photographer's own grant, or a GFDL/CC dual
#  licence. Reading only the first is how an explicitly NonCommercial,
#  NoDerivatives image was recorded as "Public domain" — an identifier with no
#  CC elements, which the NC/ND gate therefore had nothing to refuse.
# =============================================================================


FILE_PAGE_TWO_BOXES = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/9/99/Dual.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text">
    <table class="licensetpl">
      <span class="licensetpl_short">Public domain</span>
    </table>
    <table class="licensetpl">
      <span class="licensetpl_short">CC BY-NC-ND 4.0</span>
      <a href="https://creativecommons.org/licenses/by-nc-nd/4.0/">deed</a>
    </table>
  </div>
  {MW_FOOTER}
</body></html>
"""

FILE_PAGE_GFDL_AND_CC = f"""
<html><body>
  <div class="fullImageLink">
    <a href="//upload.wikimedia.org/wikipedia/commons/8/88/Dual2.jpg"><img src="x"></a>
  </div>
  <div id="mw-content-text">
    <table class="licensetpl"><span class="licensetpl_short">GFDL</span></table>
    <table class="licensetpl">
      <a href="https://creativecommons.org/licenses/by-sa/3.0/">deed</a>
    </table>
  </div>
  {MW_FOOTER}
</body></html>
"""


def _soup(html: str):
    from bs4 import BeautifulSoup

    return BeautifulSoup(html, "lxml")


def test_the_more_restrictive_of_two_licence_boxes_is_the_one_recorded():
    """
    The failure this prevents is not a wrong label, it is an unusable image
    shipped as usable: `PDM-1.0` carries no elements, so `license_permits`
    had nothing to intersect and the NC/ND gate passed it through.
    """
    from src.pipeline.normalize import license_permits, normalise_license

    licence = WikimediaCommonsSource._extract_licence(_soup(FILE_PAGE_TWO_BOXES))
    spdx = normalise_license(licence)

    assert spdx == "CC-BY-NC-ND-4.0", f"read the wrong box: {licence!r} -> {spdx}"
    assert not license_permits(spdx, ("NC", "ND")), "the gate must refuse this image"


def test_the_licence_url_comes_from_the_same_box_as_the_licence():
    """
    Read apart, the two could disagree: the licence reader skipped a CC anchor
    that yielded no code while the URL reader took the first one unconditionally.
    """
    licence = WikimediaCommonsSource._extract_licence(_soup(FILE_PAGE_TWO_BOXES))
    url = WikimediaCommonsSource._extract_licence_url(_soup(FILE_PAGE_TWO_BOXES))

    assert licence is not None and url is not None
    assert "by-nc-nd" in url, f"url points at a different box than the licence: {url}"


def test_a_readable_cc_box_beats_an_unparseable_one():
    """
    GFDL beside CC BY-SA is a real dual licence, not a conflict. Recording the
    one we can actually resolve keeps the image rather than rejecting it as
    unlicensed.
    """
    from src.pipeline.normalize import normalise_license

    licence = WikimediaCommonsSource._extract_licence(_soup(FILE_PAGE_GFDL_AND_CC))
    assert normalise_license(licence) == "CC-BY-SA-3.0"


@pytest.mark.parametrize(
    "page, expected",
    [
        (FILE_PAGE_CC, "CC-BY-SA-3.0"),
        (FILE_PAGE_CC0, "CC0-1.0"),
        (FILE_PAGE_PD, "PDM-1.0"),
    ],
)
def test_single_box_pages_are_unaffected_by_reading_all_boxes(page, expected):
    """The multi-box fix must not move any page that only ever had one box."""
    from src.pipeline.normalize import normalise_license

    assert normalise_license(WikimediaCommonsSource._extract_licence(_soup(page))) == expected
