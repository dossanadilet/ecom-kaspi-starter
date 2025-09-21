# etl/postprocess_normalize.py
import argparse, csv, re, os, sys, math, json
import pandas as pd
from unidecode import unidecode

# Примитивная транслитерация RU->slug
RU_MAP = {
    'ё':'e','й':'i','ю':'yu','я':'ya','ч':'ch','ш':'sh','щ':'sch','ж':'zh','х':'h','ц':'ts','ъ':'','ь':'',
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','з':'z','и':'i','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','ы':'y','э':'e','й':'i'
}
def ru_to_slug(s: str)->str:
    s = s.strip().lower()
    # чередуем fast: unidecode + ручные замены
    s2 = ''.join(RU_MAP.get(ch, ch) for ch in s)
    s2 = unidecode(s2)
    s2 = re.sub(r'[^a-z0-9]+','-', s2).strip('-')
    # косметика для «gb/гб»
    s2 = s2.replace('-gb', 'gb').replace('gb-', 'gb-')
    return s2

# Override RU_MAP to rely solely on Unidecode (avoid mojibake transliteration artifacts)
RU_MAP = {}

def price_to_float(x):
    if pd.isna(x): return math.nan
    s = str(x)
    m = re.search(r'(?:(?:\d{1,3}(?:[ \u00A0]\d{3})+)|\d+)(?:[.,]\d+)?', s)
    if not m: return math.nan
    return float(m.group(0).replace('\u00A0',' ').replace(' ','').replace(',','.'))

def build_url(title, pid, city="750000000"):
    if not pid or not title: return None
    slug = ru_to_slug(title)
    return f"https://kaspi.kz/shop/p/{slug}-{pid}/?c={city}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--parquet", dest="parquet", default="")
    args = ap.parse_args()

    df = pd.read_csv(args.inp, dtype=str).fillna("")
    # типизация
    for c in ["list_price","price_min","price_default","rating","reviews","offers_count"]:
        if c in df.columns:
            if c in ("rating",):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = df[c].apply(price_to_float)

    # восстановление url
    if "url" in df.columns:
        mask_bad = (df["url"]=="") & (df["product_id"]!="") & (df["title"]!="")
        df.loc[mask_bad, "url"] = df[mask_bad].apply(lambda r: build_url(r["title"], r["product_id"]), axis=1)

    # если цены пустые — подставляем list_price в min/default
    for c in ("price_min","price_default"):
        if c in df.columns and "list_price" in df.columns:
            df[c] = df[c].where(~df[c].isna(), df["list_price"])

    # убираем полностью пустые title/ids
    df = df[(df["title"]!="") & (df["product_id"]!="")].copy()

    df.to_csv(args.out, index=False)
    if args.parquet:
        os.makedirs(os.path.dirname(args.parquet) or ".", exist_ok=True)
        df.to_parquet(args.parquet, index=False)
        print(f"Parquet saved: {args.parquet} (rows={len(df)})")
    print(f"CSV saved: {args.out} (rows={len(df)})")

if __name__ == "__main__":
    main()
