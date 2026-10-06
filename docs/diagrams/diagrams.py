#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Open Data Stack's README diagrams: architecture (the solution) and tech stack (where
each tool lives), on one layout. Run it to regenerate both .excalidraw.svg files here.
The kit is Rai's /media → diagram; RAI_DIAGRAM_KIT points elsewhere if needed.
"""

import os
import sys
from collections.abc import Callable
from pathlib import Path

DEFAULT_KIT = Path.home() / "helm/03-rai/skills/media/scripts/diagram"
KIT = Path(os.environ.get("RAI_DIAGRAM_KIT", DEFAULT_KIT))
sys.path.insert(0, str(KIT))
from lib import MUTED, Scene, render  # ty: ignore[unresolved-import]

HERE = Path(__file__).parent
# zone: (x, y, w, h, title)
Z = {
    "source": (0, 150, 190, 560, "SOURCE"),
    "batch": (350, 150, 640, 220, "BATCH"),
    "stream": (350, 490, 640, 220, "STREAM"),
    "warehouse": (1110, 150, 280, 560, "WAREHOUSE"),
    "visualize": (1510, 150, 300, 560, "VISUALIZE"),
    "checks": (300, 820, 1560, 190, "CHECKS"),
}
BATCH, STREAM, MID = 260, 600, 430


def frame(s: Scene, header: Callable[[Scene], None]) -> None:
    """Zones, the project box and the labelled arrows: the same in both diagrams."""
    s.zone(300, 0, 1560, 750, None, dotted=True)
    header(s)
    for x, y, w, h, title in Z.values():
        s.zone(x, y, w, h, title)
    s.arrow([(192, BATCH), (348, BATCH)], label="fetch\ndaily bars",
            at=(192, BATCH - 60, 106))
    s.arrow([(192, STREAM), (348, STREAM)], label="poll\nlive quotes",
            at=(192, STREAM - 60, 106))
    s.arrow([(992, BATCH), (1108, BATCH)], label="upsert\ndaily bars",
            at=(990, BATCH - 60, 120))
    s.arrow([(992, STREAM), (1108, STREAM)], label="append\nlive prices",
            at=(990, STREAM - 60, 120))
    s.arrow([(1392, MID), (1508, MID)], label="query\nwith SQL", at=(1390, MID - 60, 120))
    s.arrow([(400, 818), (400, 752)], color=MUTED, dashed=True,
            label="check\nthe project", at=(410, 760, 130))


def row(
    s: Scene, zone: str, items: list[tuple[str, str]], top: int | None = None
) -> None:
    """Logos with names, spread evenly across a zone."""
    x, y, w, _, _ = Z[zone]
    slot = w / len(items)
    top = y + 70 if top is None else top
    for j, (key, name) in enumerate(items):
        s.tool(x + slot * j + slot / 2, top, key, name, w=slot)


def tech_stack() -> None:
    s = Scene()

    def header(s: Scene) -> None:
        s.tool(376, 14, "si:uv", "uv", w=110)
        s.tool(520, 14, "si:docker", "Docker Compose", w=170)

    frame(s, header)
    row(s, "source", [("si:json", "Yahoo Finance")], top=MID - 50)
    row(s, "batch", [("si:apacheairflow", "Apache Airflow"), ("si:pandas", "pandas")])
    row(s, "stream", [("si:apachekafka", "Apache Kafka"), ("si:apachespark", "Apache Spark")])
    row(s, "warehouse", [("gh:duckdb", "DuckDB"), ("si:pydantic", "Pydantic")], top=MID - 50)
    row(s, "visualize", [("si:apachesuperset", "Apache Superset")], top=MID - 50)
    row(s, "checks", [("si:ruff+#261230", "Ruff"), ("si:pytest", "pytest")], top=870)
    s.save("tech-stack")


def architecture() -> None:
    s = Scene()

    frame(s, lambda s: None)

    def stack(zone: str, labels: list[str], mid: int, h: int = 52, gap: int = 14) -> None:
        """Part boxes stacked down a zone, centred on mid."""
        x, _, w, _, _ = Z[zone]
        top = mid - (len(labels) * (h + gap) - gap) // 2
        for label in labels:
            s.part(x + 20, top, w - 40, h, label)
            top += h + gap

    def across(zone: str, labels: list[str], top: int, h: int = 52, gap: int = 14) -> None:
        """Part boxes side by side across a zone, read left to right."""
        x, _, w, _, _ = Z[zone]
        pw = (w - 40 - gap * (len(labels) - 1)) / len(labels)
        for j, label in enumerate(labels):
            s.part(x + 20 + j * (pw + gap), top, pw, h, label)

    stack("source", ["Yahoo Finance", "5 tickers", "no API key"], MID)
    across("batch", ["daily DAG, 06:00 UTC", "backfill DAG, manual"], top=205)
    across("batch", ["extract", "validate", "load", "report"], top=271)
    across("stream", ["publish quotes", "price topic", "batch consumer"], top=545)
    x, _, w, _, _ = Z["stream"]
    s.part(x + w / 2 - 95, 611, 190, 52, "windowed stats")
    for label, mid in [("daily_aggregates", BATCH), ("stocks", MID), ("stock_prices", STREAM)]:
        stack("warehouse", [label], mid)
    stack("visualize", ["price trends", "trading volume", "top performers",
                        "latest prices"], MID)
    x, y, w, _, _ = Z["checks"]
    checks = ["format, lint", "types", "tests, network mocked"]
    slot = (w - 40) / len(checks)
    for j, label in enumerate(checks):
        s.part(x + 20 + slot * j + 8, y + 70, slot - 16, 64, label)
    s.save("architecture")


tech_stack()
architecture()
render(HERE, "architecture", "tech-stack")
