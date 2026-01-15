#!/usr/bin/env python3
"""
Fetch Steam news (GetNewsForApp) for a list of tickers and store into a single steam_data.csv file.

Usage example:
  python src/apis/steam/fetch_steam_data.py \
    --config src/config/Direct-cd.yaml \
    --start-date 2025-09-25 \
    --end-date 2025-11-15 \
    --limit 15

"""

import argparse
import csv
import os
from datetime import datetime, timedelta
from typing import List
import yaml

from apis.steam.api import SteamAPI
from apis.common_model import MediaNews
from util.logger import logger


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def fetch_steam_news(
    ticker: str,
    trading_date: datetime,
    limit: int = 15,
) -> List[MediaNews]:
    """Fetch Steam news for a given ticker and trading_date."""
    api = SteamAPI()
    return api.get_item_relevant_news(
        item_name=ticker,
        count=limit,
        trading_date=trading_date,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Steam news and build steam_data.csv")
    parser.add_argument("--config", required=True, help="Path to config yaml (with exp_name and tickers)")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--limit", type=int, default=15, help="Max news items per ticker per day")
    default_output = os.path.join(os.path.dirname(__file__), "steam_data.csv")
    parser.add_argument("--output", type=str, default=default_output, help="Output CSV path")
    return parser.parse_args()


def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_rows(csv_path: str, rows: List[dict]):
    ensure_dir(os.path.dirname(csv_path))
    header = [
        "trading_date",
        "ticker",
        "title",
        "publish_time",
        "publisher",
        "link",
        "summary",
        "score",
        "num_comments",
    ]
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    cfg = load_config(args.config)
    exp_name = cfg.get("exp_name", "default_exp")
    tickers: List[str] = cfg.get("tickers", [])
    if not tickers:
        logger.error("No tickers found in config.")
        return

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    logger.info(f"Fetching Steam news for exp={exp_name}, dates {args.start_date} -> {args.end_date}, tickers={len(tickers)}")

    total_rows = 0
    for d in daterange(start_date, end_date):
        for t in tickers:
            news_list = fetch_steam_news(ticker=t, trading_date=d, limit=args.limit)
            rows = []
            for n in news_list:
                rows.append(
                    {
                        "trading_date": d.strftime("%Y-%m-%d"),
                        "ticker": t,
                        "title": n.title,
                        "publish_time": n.publish_time,
                        "publisher": n.publisher,
                        "link": n.link or "",
                        "summary": n.summary or "",
                        "score": n.score,
                        "num_comments": n.num_comments,
                    }
                )
            write_rows(args.output, rows)
            total_rows += len(rows)
            logger.info(f"[{d.date()}] {t}: saved {len(rows)} Steam news")

    logger.info(f"Done. Total rows saved: {total_rows}, output={args.output}")


if __name__ == "__main__":
    main()

