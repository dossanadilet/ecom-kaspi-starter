"""
ETL orchestrator for Kaspi market snapshot.

Pipeline:
  1) For configured topics, call the scraper to produce per-topic CSVs (tmp/).
  2) Merge and de-duplicate into a raw CSV.
  3) Normalize fields and rebuild product URLs.
  4) Save artifacts under data/latest/ and data/daily/.

This script is intentionally self-contained and robust:
  - Falls back to sensible defaults if config is missing/empty.
  - Creates directories on demand.
  - Skips topics that fail to scrape instead of aborting whole run.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests

# Local imports
try:
    from etl import batch_runner
    from etl import postprocess_normalize as ppn
except Exception:
    # Fallback when executed as a file (python etl/run_etl.py)
    import importlib
    HERE = Path(__file__).resolve().parent
    ROOT = HERE.parent
    for p in (str(ROOT), str(HERE)):
        if p not in sys.path:
            sys.path.insert(0, p)
    try:
        batch_runner = importlib.import_module("etl.batch_runner")
        ppn = importlib.import_module("etl.postprocess_normalize")
    except Exception:
        batch_runner = importlib.import_module("batch_runner")
        ppn = importlib.import_module("postprocess_normalize")


DEFAULT_TOPICS: List[Tuple[str, str]] = batch_runner.DEFAULT_TOPICS


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def discover_topics(config_path: Path | None) -> List[Tuple[str, str]]:
    """Load topics from YAML if present, otherwise use defaults.

    YAML structure examples:
      topics:
        - query: "смартфоны"
          category: "smartphones"
        - query: "iphone"
          category: "smartphones"
      pages: 6
      delay: 0.9
      split_by_brand: false
      max_items: 1000
    """
    if not config_path or not config_path.exists() or config_path.stat().st_size == 0:
        return DEFAULT_TOPICS

    try:
        import yaml  # type: ignore
    except Exception:
        # YAML not installed? fall back to defaults
        return DEFAULT_TOPICS

    try:
        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        raw_topics = data.get("topics") or []
        topics: List[Tuple[str, str]] = []
        for t in raw_topics:
            if isinstance(t, dict):
                q = str(t.get("query", "")).strip()
                cat = str(t.get("category", "smartphones")).strip() or "smartphones"
                if q:
                    topics.append((q, cat))
            elif isinstance(t, str):
                # allow "query:category" short form
                parts = (t.split(":", 1) + ["smartphones"])[:2]
                q = parts[0].strip()
                cat = parts[1].strip() or "smartphones"
                if q:
                    topics.append((q, cat))
        return topics or DEFAULT_TOPICS
    except Exception:
        return DEFAULT_TOPICS


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna("")
    # numeric-like columns
    for c in ["list_price", "price_min", "price_default", "rating", "reviews", "offers_count"]:
        if c in df.columns:
            if c in ("rating",):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = df[c].apply(ppn.price_to_float)

    # rebuild URL if missing but we have title + id
    if "url" in df.columns:
        mask_bad = (df["url"] == "") & (df.get("product_id", "") != "") & (df.get("title", "") != "")
        if mask_bad.any():
            df.loc[mask_bad, "url"] = df[mask_bad].apply(
                lambda r: ppn.build_url(r.get("title", ""), r.get("product_id", "")), axis=1
            )

    # default min/default to list_price when empty
    for c in ("price_min", "price_default"):
        if c in df.columns and "list_price" in df.columns:
            df[c] = df[c].where(~df[c].isna(), df["list_price"])

    # keep only rows with id + title
    if "product_id" in df.columns and "title" in df.columns:
        df = df[(df["title"] != "") & (df["product_id"] != "")].copy()

    return df


def _tg_send(text: str, parse_mode: str = "HTML") -> bool:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            data={
                "chat_id": chat_id,
                "text": text[:4000],
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        return 200 <= r.status_code < 300
    except Exception as e:
        print(f"WARN: telegram send failed: {e}")
        return False


def _tg_send_file(path: Path, caption: str = "") -> bool:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id or not path.exists():
        return False
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with path.open("rb") as fh:
            files = {"document": (path.name, fh, "text/csv")}
            data = {"chat_id": chat_id, "caption": caption}
            r = requests.post(url, files=files, data=data, timeout=60)
        return 200 <= r.status_code < 300
    except Exception as e:
        print(f"WARN: telegram send document failed: {e}")
        return False


def notify_telegram(df: pd.DataFrame, latest_path: Path, daily_path: Path) -> None:
    if df.empty:
        return
    total = len(df)
    unique_products = df["product_id"].nunique() if "product_id" in df.columns else total
    unique_topics = df["topic"].nunique() if "topic" in df.columns else 0
    lines = [
        "<b>Kaspi ETL</b>",
        f"Всего записей: <b>{total}</b> (SKU: {unique_products})",
    ]
    if unique_topics:
        top_topics = df.groupby("topic")["product_id"].nunique().sort_values(ascending=False).head(5)
        lines.append("ТОП запросов:")
        for topic, cnt in top_topics.items():
            lines.append(f"• {topic}: {cnt}")
    if "price_min" in df.columns:
        tmp = df.copy()
        tmp["price_min"] = pd.to_numeric(tmp["price_min"], errors="coerce")
        best = tmp.dropna(subset=["price_min"]).sort_values("price_min").head(5)
        if not best.empty:
            lines.append("Самые доступные предложения:")
            for _, row in best.iterrows():
                title = str(row.get("title", ""))
                if len(title) > 60:
                    title = title[:60] + "…"
                price = float(row["price_min"])
                lines.append(f"• {title} — {price:,.0f} ₸")
    _tg_send("\n".join(lines))
    _tg_send_file(latest_path, caption="market_snapshot.csv")
    # optionally send daily copy if different name
    if daily_path != latest_path:
        _tg_send_file(daily_path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Kaspi ETL and produce market snapshot artifacts")
    ap.add_argument("--config", default="etl/config.yaml")
    ap.add_argument("--pages", type=int, default=6)
    ap.add_argument("--delay", type=float, default=0.9)
    ap.add_argument("--max-items", type=int, default=1000)
    ap.add_argument("--split-by-brand", action="store_true")
    ap.add_argument("--out-latest", dest="out_latest", default="data/latest/market_snapshot.csv")
    ap.add_argument("--daily-dir", dest="daily_dir", default="data/daily")
    args = ap.parse_args()

    config_path = Path(args.config)
    topics = discover_topics(config_path)

    ensure_dir("tmp")
    frames: List[pd.DataFrame] = []
    for q, cat in topics:
        out_path = Path("tmp") / f"{cat}_{q}.csv"
        # Run scraper via the helper (subprocess to the real scraper)
        try:
            batch_runner.run_scrape(
                q=q,
                out=str(out_path),
                pages=args.pages,
                delay=args.delay,
                mode="both",
                brands=args.split_by_brand,
                max_items=args.max_items,
            )
        except Exception as e:
            print(f"WARN: scrape failed for '{q}' in '{cat}': {e}")
            continue

        if out_path.exists():
            try:
                df = pd.read_csv(out_path, dtype=str)
                df["topic"] = q
                df["category_hint"] = cat
                frames.append(df)
            except Exception as e:
                print(f"WARN: failed reading {out_path}: {e}")

    if not frames:
        print("No data collected; aborting without artifacts.")
        sys.exit(1)

    raw = pd.concat(frames, ignore_index=True)
    if set(["product_id", "title"]).issubset(raw.columns):
        raw = raw.drop_duplicates(subset=["product_id", "title"]).copy()

    # Normalize
    final = normalize_df(raw)

    # Artifacts
    latest_path = Path(args.out_latest)
    ensure_dir(latest_path.parent)
    final.to_csv(latest_path, index=False, encoding="utf-8")
    print(f"Saved latest snapshot: {latest_path} rows={len(final)}")

    # Daily dated copy
    ensure_dir(args.daily_dir)
    day = dt.date.today().strftime("%Y-%m-%d")
    daily_path = Path(args.daily_dir) / f"market_snapshot_{day}.csv"
    final.to_csv(daily_path, index=False, encoding="utf-8")
    print(f"Saved daily snapshot:  {daily_path} rows={len(final)}")

    if os.getenv("TG_BOT_TOKEN") and os.getenv("TG_CHAT_ID"):
        notify_telegram(final, latest_path, daily_path)


if __name__ == "__main__":
    main()
