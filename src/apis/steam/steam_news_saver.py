"""
Save Steam news items to CSV for later review.
"""

import os
import pandas as pd
from datetime import datetime
from typing import List, Optional
from apis.common_model import MediaNews
from util.logger import logger


def save_steam_news_to_csv(
    news_list: List[MediaNews],
    ticker: str,
    trading_date: datetime,
    data_source_type: str = "ticker-relevant",
    exp_name: Optional[str] = None,
    csv_path: Optional[str] = None,
):
    """Save Steam news to CSV."""
    if csv_path is None:
        if exp_name:
            safe_exp_name = "".join(c for c in exp_name if c.isalnum() or c in ("-", "_")).strip()
            csv_filename = f"steam_news_{safe_exp_name}.csv"
        else:
            csv_filename = "steam_news.csv"
        csv_path = os.path.join(os.path.dirname(__file__), csv_filename)

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    rows = []
    for n in news_list:
        rows.append(
            {
                "trading_date": trading_date.strftime("%Y-%m-%d") if trading_date else "",
                "ticker": ticker,
                "data_source_type": data_source_type,
                "title": n.title,
                "publish_time": n.publish_time,
                "publisher": n.publisher,
                "link": n.link or "",
                "summary": n.summary or "",
                "score": n.score,
                "num_comments": n.num_comments,
            }
        )

    if not rows:
        logger.debug(f"No Steam news to save for {ticker} on {trading_date}")
        return

    df = pd.DataFrame(rows)

    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
        df = pd.concat([existing_df, df], ignore_index=True)
        df = df.drop_duplicates(
            subset=["trading_date", "ticker", "title", "publish_time"], keep="last"
        )

    df.to_csv(csv_path, index=False)
    logger.info(f"Saved {len(rows)} Steam news to {csv_path} for {ticker} on {trading_date}")



