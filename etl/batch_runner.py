import argparse, os, time, subprocess, sys, pandas as pd

DEFAULT_TOPICS = [
    ("смартфон", "smartphones"),
    ("iphone", "smartphones"),
    ("samsung", "smartphones"),
    ("xiaomi", "smartphones"),
]

def run_scrape(q, out, pages=6, delay=0.9, mode="search", brands=False, max_items=800):
    cmd = [
        sys.executable, "etl/scrape_kaspi.py",
        "--query", q, "--pages", str(pages),
        "--out", out, "--delay", str(delay),
        "--mode", mode, "--max-items", str(max_items),
        "--no-zone"
    ]
    if brands: cmd.append("--split-by-brand")
    print("RUN:", " ".join(cmd))
    subprocess.check_call(cmd)

# Override DEFAULT_TOPICS with clean Cyrillic to avoid mojibake issues
DEFAULT_TOPICS = [
    ("смартфоны", "smartphones"),
    ("iphone", "smartphones"),
    ("samsung", "smartphones"),
    ("xiaomi", "smartphones"),
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", default="")
    ap.add_argument("--out", default="data/kaspi_all.csv")
    ap.add_argument("--pages", type=int, default=6)
    ap.add_argument("--brands", action="store_true")
    ap.add_argument("--max-items", type=int, default=1000)
    args = ap.parse_args()

    topics = DEFAULT_TOPICS
    if args.topics.strip():
        # формат: "запрос1:cat1,запрос2:cat2,..."
        topics = []
        for chunk in args.topics.split(","):
            q, cat = (chunk.split(":") + ["smartphones"])[:2]
            topics.append((q.strip(), cat.strip()))

    os.makedirs("tmp", exist_ok=True)
    frames = []
    for q, cat in topics:
        out = f"tmp/{cat}_{q}.csv".replace(" ", "_")
        run_scrape(q, out, pages=args.pages, brands=args.brands, max_items=args.max_items)
        df = pd.read_csv(out, dtype=str)
        df["topic"] = q
        df["category_hint"] = cat
        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["product_id","title"])
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    all_df.to_csv(args.out, index=False, encoding="utf-8")
    print("Saved merged:", args.out, "rows=", len(all_df))

if __name__ == "__main__":
    main()
