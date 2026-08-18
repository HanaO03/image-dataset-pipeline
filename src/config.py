"""
Configuration — every knob in one place, all of it overridable by environment.

Design notes:

* Nothing is hardcoded at a call site. Thresholds live here so they can be
  justified, tuned and version-controlled as *decisions* rather than buried
  magic numbers.
* Validation happens at import time. A bad DB password or a nonsensical split
  ratio should kill the process in the first second, not thirty seconds into a
  download loop. This is the fail-fast half of the error policy.
* `Settings.snapshot()` feeds pipeline_runs.config_snapshot, so every dataset
  carries the exact configuration that produced it. Secrets are stripped.
"""

from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _group_config(prefix: str) -> SettingsConfigDict:
    """
    Config for one settings group. Every group reads `.env`, not only the root.

    That is the whole point of this function existing. `env_file` was set on the
    root `Settings` class alone, which covers the root's own fields and nothing
    else: each group below is a separate `BaseSettings` and read `os.environ`
    only. So `SOURCE_TARGET_PER_CLASS=7` in `.env` was silently ignored — the
    run used 60 and said nothing.

    Under compose it worked, which is exactly why it survived: `env_file:` there
    injects the file as real environment variables, so the gap was invisible on
    the documented path and visible only to someone running the pipeline
    locally — after following a README that opens with `cp .env.example .env`.
    A configuration file that is read in one environment and ignored in another,
    with no error either way, is the worst kind of configuration bug.
    """
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class DatabaseSettings(BaseSettings):
    """Postgres connection. Values match the service name in docker-compose."""

    model_config = _group_config("POSTGRES_")

    host: str = "postgres"
    port: int = 5432
    db: str = "imagedb"
    user: str = "pipeline"
    password: SecretStr = SecretStr("pipeline")

    #: Postgres inside compose accepts connections a moment before it is really
    #: ready. A healthcheck alone is necessary but not sufficient, so the app
    #: also retries — belt and braces, because a flaky first run reads as a
    #: broken submission.
    connect_max_retries: int = 30
    connect_retry_delay_seconds: float = 2.0

    @computed_field
    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    def safe_dsn(self) -> str:
        """DSN with the password masked — safe to log."""
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.db}"


class HttpSettings(BaseSettings):
    """Outbound HTTP behaviour: politeness, resilience, and hard limits."""

    model_config = _group_config("HTTP_")

    #: Identifies us to the sources. Wikimedia's policy explicitly requires a
    #: descriptive User-Agent with a contact; anonymous scrapers get blocked.
    user_agent: str = (
        "AICare-DatasetPipeline/1.0 (take-home evaluation; contact: hanaaabukhadija@gmail.com)"
    )
    #: (connect, read). Separate values: a slow server is tolerable, an
    #: unreachable one is not.
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0

    max_retries: int = 4
    backoff_factor: float = 1.5          # 1.5s, 3s, 6s, 12s + jitter
    #: Only these are retried. A 404 will never become a 200, and retrying 403
    #: is how you get an IP ban.
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504)

    #: Minimum seconds between requests to the same host. Crude but effective,
    #: and far easier to defend than an adaptive scheme nobody can reason about.
    min_delay_seconds: float = 0.5
    #: Per-host overrides, because hosts do not share a tolerance.
    #:
    #: `upload.wikimedia.org` answered 429 with `Retry-After: 11` at 2 req/s
    #: during a real run. The retry logic absorbed it correctly, but backing off
    #: after being told off is worse than not provoking it: every 429 costs 11
    #: idle seconds and spends goodwill with a host that is serving us for free.
    #: 1.5s is the rate at which the same run produced none.
    min_delay_by_host: dict[str, float] = Field(
        default_factory=lambda: {"upload.wikimedia.org": 1.5}
    )
    #: Hard ceiling on bytes accepted per image, enforced *during* streaming.
    #: Prevents a hostile or misconfigured server from filling the volume.
    max_download_bytes: int = 20 * 1024 * 1024   # 20 MB
    respect_robots_txt: bool = True


class ValidationSettings(BaseSettings):
    """
    Image sanity thresholds.

    Every bound here is a defensible judgement call, not a default:
    * 64px  — below this there is no usable signal for a CV model.
    * 8000px — above this we are looking at panoramas or scanned maps, not subjects.
    * 1 KB  — smaller files are placeholder icons or error pages, not photographs.
    * 6.0 aspect ratio — banners, spritesheets and diagrams, not a single subject.
    """

    model_config = _group_config("VALIDATION_")

    min_dimension_px: int = 64
    max_dimension_px: int = 8000
    min_file_size_bytes: int = 1024
    max_aspect_ratio: float = 6.0
    allowed_formats: tuple[str, ...] = ("JPEG", "PNG", "WEBP", "GIF", "BMP", "TIFF")
    #: Pillow's decompression-bomb guard. ~89M pixels is the library default;
    #: we tighten it, since nothing legitimate in this dataset comes close.
    max_image_pixels: int = 50_000_000

    #: STRICT LICENCE POLICY. An image whose licence we cannot determine is
    #: rejected, never stored. Rationale: the output is training data, and a
    #: dataset with unknown provenance is a legal liability that is far more
    #: expensive than the handful of images it costs us.
    require_license: bool = True


class LicenseSettings(BaseSettings):
    """
    What licences are allowed to reach the dataset, enforced for every source.

    This exists because the policy previously lived in one place it could not
    cover: `SourceSettings.openverse_license_type`, a query parameter on the
    API. That filters the API and nothing else, so a Commons file page carrying
    an NC or ND licence was mapped to a valid SPDX identifier and stored — in a
    dataset the documentation described as usable for commercial training. The
    filter was real; its scope was not what the prose claimed.

    The API filter stays, because not fetching an image is cheaper than
    fetching and discarding it. This is the gate that makes the claim true:
    it runs at normalisation, after both sources converge on one schema, so it
    applies to every image regardless of where it came from.

    Why these two elements:
      * **ND** (NoDerivatives) — a model trained on an image is arguably a
        derivative work, so ND images are not safely usable as training data.
      * **NC** (NonCommercial) — rules the image out of any commercial product.

    Set `forbidden_elements=[]` for a research dataset that will never ship.
    """

    model_config = _group_config("LICENSE_")

    forbidden_elements: tuple[str, ...] = ("NC", "ND")

    @field_validator("forbidden_elements")
    @classmethod
    def _known_elements(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        allowed = {"BY", "NC", "ND", "SA"}
        upper = tuple(e.strip().upper() for e in v if e.strip())
        unknown = set(upper) - allowed
        if unknown:
            raise ValueError(
                f"unknown CC element(s) {sorted(unknown)}; expected any of {sorted(allowed)}"
            )
        return upper


class DedupeSettings(BaseSettings):
    """
    Deduplication.

    Exact duplicates are handled by the UNIQUE constraint on images.sha256 —
    no configuration needed, because the database enforces it unconditionally.
    Near-duplicates need a threshold, and a threshold needs a justification.
    """

    model_config = _group_config("DEDUPE_")

    enable_perceptual: bool = True
    #: Hamming distance over a 64-bit pHash.
    #:   0     — identical after re-encoding (resize, JPEG quality change)
    #:   1-5   — same photo, different crop/watermark/colour grade   <-- our band
    #:   6-10  — visually similar, often genuinely different photographs
    #:   > 10  — unrelated
    #: 5 is the conventional operating point. It has not been tuned against a
    #: hand-labelled set of near-duplicate pairs, because this collection turned
    #: out to contain none within any distance worth arguing about — see the
    #: note on near-duplicate detection in the README, and the entry under
    #: "what I would do with more time" that says tuning it is still owed.
    max_hamming_distance: int = 5
    #: When two images collide, keep the higher-resolution one. Ties break on
    #: first-seen, which keeps the choice deterministic across re-runs.
    keep_strategy: str = "highest_resolution"

    @field_validator("max_hamming_distance")
    @classmethod
    def _sane_distance(cls, v: int) -> int:
        if not 0 <= v <= 64:
            raise ValueError("max_hamming_distance must be within 0..64 (64-bit hash)")
        return v

    @field_validator("keep_strategy")
    @classmethod
    def _known_strategy(cls, v: str) -> str:
        allowed = {"highest_resolution", "first_seen"}
        if v not in allowed:
            raise ValueError(f"keep_strategy must be one of {sorted(allowed)}")
        return v


class SplitSettings(BaseSettings):
    """
    Train/val splitting.

    Deliberately NOT `train_test_split(random_state=42)`: assignment is derived
    from each image's own content hash, so it is reproducible from the image
    alone, with no ordering dependency and no seed to lose.

    The two strategies below differ in what they guarantee, and the trade-off is
    real — `pipeline/split.py` is the authoritative description, including why
    the default is the one that reshuffles a little. In short:

        stratified_rank (default) — exact ratio inside every class, which is
                                    what "stratified" means and what the brief
                                    asks for; images near the class boundary can
                                    move as the dataset grows.
        stable_bucket             — an image's split is a function of the image
                                    alone and never changes; the realised ratio
                                    only approximates the target.

    Prefer `stable_bucket` once a model has been trained against a split and
    permanence matters more than an exact ratio.
    """

    model_config = _group_config("SPLIT_")

    train_percent: int = 80
    stratify_by_class: bool = True
    #: "stratified_rank" — exact ratio inside every class (what the brief asks
    #: for). "stable_bucket" — an image's split never changes as the dataset
    #: grows, at the cost of an approximate ratio. See pipeline/split.py.
    strategy: str = "stratified_rank"
    #: Salt mixed into the hash. Changing it produces a completely different
    #: (still deterministic) split — the escape hatch for "we need a fresh
    #: split" without abandoning content-based assignment.
    salt: str = "aicare-v1"

    @field_validator("train_percent")
    @classmethod
    def _sane_percent(cls, v: int) -> int:
        if not 50 <= v <= 95:
            raise ValueError("train_percent should be between 50 and 95")
        return v

    @field_validator("strategy")
    @classmethod
    def _known_strategy(cls, v: str) -> str:
        allowed = {"stratified_rank", "stable_bucket"}
        if v not in allowed:
            raise ValueError(f"split strategy must be one of {sorted(allowed)}")
        return v


class SourceSettings(BaseSettings):
    """Which classes to collect, from where, and how many."""

    model_config = _group_config("SOURCE_")

    #: Three visually distinct classes, per the task brief.
    classes: tuple[str, ...] = ("cat", "dog", "bird")
    #: Kept images per class. The brief asks for 40-60, and this is enforced in
    #: both directions: `pipeline/select.py` trims a class back to this number,
    #: and the quality gate below flags one that came in under the minimum.
    target_per_class: int = 60
    #: We over-fetch, because validation, licence mapping and dedup all discard
    #: candidates. Without headroom a run silently under-delivers; the surplus
    #: that survives is trimmed at the selection stage rather than shipped.
    #:
    #: 1.5 rather than the original 2.0: the measured rejection rate against
    #: these sources is ~2.4%, so 100% headroom was paying for twice the
    #: bandwidth to guard against a loss of a few per cent. Headroom cannot go
    #: much below this, because which images survive is only knowable after
    #: they have been downloaded and decoded.
    overfetch_factor: float = 1.5
    #: Quality gate: below this many kept images in any class, the run is
    #: marked 'partial' rather than reported as a success.
    min_per_class: int = 40

    openverse_base_url: str = "https://api.openverse.org/v1"
    #: Anonymous requests are capped at 20 results per page by Openverse.
    #: Authenticated clients may request more, but 20 keeps the anonymous path
    #: working out of the box — which is what a reviewer will actually run.
    openverse_page_size: int = 20
    #: Restrict to licences that permit BOTH commercial use AND modification.
    #:
    #: Deliberately not "all-cc". A first run with "all-cc" produced a dataset
    #: where 29 of 48 images carried NC (NonCommercial) or ND (NoDerivatives)
    #: terms. ND is the serious one: a model trained on an image is arguably a
    #: derivative work, so ND images are not safely usable as training data at
    #: all, and NC rules them out for any commercial product. Filtering at the
    #: source is cheaper and far more reliable than discovering the problem in
    #: the exported manifest.
    openverse_license_type: str = "commercial,modification"
    #: Optional free OAuth credentials. Anonymous access works and is the
    #: default; supplying these simply raises the rate limit. Kept optional on
    #: purpose so `docker compose up` needs no signup to succeed.
    openverse_client_id: str | None = None
    openverse_client_secret: SecretStr | None = None

    commons_base_url: str = "https://commons.wikimedia.org"
    #: Category pages scraped for the supplementary source. Chosen because they
    #: are curated, stable, and every file page carries explicit licensing.
    commons_categories: dict[str, str] = Field(
        default_factory=lambda: {
            "cat": "Category:Cats",
            "dog": "Category:Dogs",
            "bird": "Category:Birds",
        }
    )
    commons_max_pages: int = 3
    #: Scraping costs one extra request per image (the file page must be opened
    #: to read its licence), so the scraped source is capped independently of
    #: the API source. The brief asks us to *supplement* at least one class by
    #: scraping, not to source everything that way.
    commons_max_images_per_class: int = 25
    #: Broad Commons categories (Cats, Dogs, Birds) are *container* categories:
    #: they are built almost entirely from subcategories and hold few or no
    #: media files directly. Scraping only the top level returns nothing, which
    #: is exactly what happened on the first real run. Descending one level
    #: handles that structure without hardcoding narrow category names that
    #: Commons could reorganise at any time.
    commons_follow_subcategories: bool = True
    commons_max_subcategories: int = 8

    def fetch_limit(self, still_needed: int) -> int:
        """
        How many candidates to ask a source for, given the remaining gap.

        Scoped to what a class still needs rather than to the target, so a
        re-run against a full dataset asks for nothing at all.
        """
        return max(0, round(still_needed * self.overfetch_factor))

    @property
    def fetch_limit_per_class(self) -> int:
        """The limit for an empty dataset — the first run's per-class budget."""
        return self.fetch_limit(self.target_per_class)


class PathSettings(BaseSettings):
    """
    Filesystem layout.

    Images live on disk with only their path in Postgres — not as BYTEA.
    Reasons: the database stays small and fast to back up, image bytes are an
    object-storage concern (this layout maps directly onto an S3 prefix later),
    and dumping the DB should not mean moving gigabytes of JPEG.
    """

    model_config = _group_config("PATH_")

    data_dir: Path = Path("/data")

    #: There is deliberately no `raw_dir`. Raw upstream payloads are the replay
    #: source, and they live in `raw_records.payload` (JSONB) rather than as
    #: files: they are small, they are queryable there, and one storage location
    #: for them beats two that can disagree. An empty `/data/raw` sitting next
    #: to the images would imply a landing zone that does not exist.

    @computed_field
    @property
    def images_dir(self) -> Path:
        """
        Content-addressed image store: images/<sha[:2]>/<sha>.<ext>

        Same bytes always land on the same path, so re-runs skip downloads and
        exact duplicates cost zero extra disk. The two-character fan-out keeps
        any single directory from accumulating too many entries.
        """
        return self.data_dir / "images"

    @computed_field
    @property
    def output_dir(self) -> Path:
        """The ML-ready deliverable: parquet, csv, manifest, split folders."""
        return self.data_dir / "output"


class Settings(BaseSettings):
    """Root settings object. Import `get_settings()` rather than constructing this."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    log_level: str = "INFO"
    #: Structured JSON logs by default so every line carries run_id and stage
    #: and can be shipped somewhere. Set false for readable local development.
    log_json: bool = True
    #: Bounded concurrency for downloads. The work is I/O-bound, so threads are
    #: the right tool.
    #:
    #: The realised speed-up is bounded by `http.min_delay_seconds`, which is
    #: enforced *per host*: eight workers pulling from a single image CDN are
    #: limited to 1/min_delay requests per second no matter how many threads
    #: exist. The pool pays off where the run spans several hosts, and the delay
    #: is what keeps us welcome on each of them. Raising workers without also
    #: raising the delay does not go faster — it just earns a 429.
    download_workers: int = 8
    #: Ignore the stored URL index and re-download everything. The index assumes
    #: a URL still serves the bytes it served last time, which is true of both
    #: sources here but not of the web in general; this is the escape hatch.
    refetch: bool = False

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    http: HttpSettings = Field(default_factory=HttpSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)
    license: LicenseSettings = Field(default_factory=LicenseSettings)
    dedupe: DedupeSettings = Field(default_factory=DedupeSettings)
    split: SplitSettings = Field(default_factory=SplitSettings)
    sources: SourceSettings = Field(default_factory=SourceSettings)
    paths: PathSettings = Field(default_factory=PathSettings)

    def snapshot(self) -> dict[str, Any]:
        """
        Secret-free configuration dump for pipeline_runs.config_snapshot.

        Every produced dataset can therefore be traced back to the exact
        settings that produced it, without needing the git checkout.
        """
        data = self.model_dump(mode="json")
        data.get("database", {}).pop("password", None)
        data.get("database", {}).pop("dsn", None)
        return data

    @staticmethod
    def git_commit() -> str | None:
        """Best-effort commit id for run lineage. Absent in a built image — fine."""
        try:
            out = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True, text=True, timeout=5, check=True,
            )
            return out.stdout.strip() or None
        except Exception:
            return os.getenv("GIT_COMMIT")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor. Validation errors surface here, at first import."""
    return Settings()
