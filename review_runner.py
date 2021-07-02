from openrice.spiders.reviewspider import OpenriceSpider
import pandas as pd
import argparse
import logging
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
import os
from datetime import datetime

HOMEDIR = os.path.expanduser("~")
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/logs/reivew_runner_{DATETIMENOW}.log",
    level=logging.DEBUG,
)

def main(folder, start, finish):
    logging.info(f"Command: --folder {folder} --start {start} --finish {finish}")
    d = fr"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/data/shop_meta/{folder}/"
    settings = get_project_settings()
    df_ls = [pd.read_csv(f"{d}{f}", encoding="utf-8") for f in os.listdir(d) if "csv" in f]
    df = pd.concat(df_ls)
    df = df.sort_values("id")

    df.id = df.id.astype(int)
    df = df[["id", "reviews_url"]]
    logging.info(f"Started with {len(df)} rows")
    if not finish:
        finish = max(df.id)+1
    # logging.infof"finish: {finish}")
    # logging.infof"start: {start}")
    df.id = df.id.astype(int)
    # df = df[(df["id"] < finish) & (df["id"] >= start)]
    df = df.iloc[start:finish]
    logging.info(f"Reduced to {len(df)} rows")
    feed = f"data/reviews/{folder}"
    logging.info(f"Hoping to save to {feed}")
    if not os.path.exists(feed):
        os.makedirs(feed)
    min_id = min(df.id)
    max_id = max(df.id)
    settings["FEEDS"] = {
            f"{feed}/{folder}_id{min_id}-{max_id}_start_{start}-{finish}.csv": {
            "format": "csv",
            # "encoding": "utf8",
            "overwrite": True,
            }
    }
    # runner = CrawlerRunner(settings)
    runner = CrawlerProcess(settings=settings)
    # runner.crawl(OpenriceSpider, df=df, name = "ReviewSpider")
    logging.info("kicking off spider")
    runner.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--folder",
        help="should be blue/yellow/green_shop/eat/chains.",
        type=str,
        nargs="?",
        default=None
    )
    
    parser.add_argument(
        "--start",
        help="index to start from.",
        type=int,
        nargs="?",
        default=0
    )
    parser.add_argument(
        "--finish",
        help="index to end from.",
        type=int,
        nargs="?",
        default=None
    )
    args = parser.parse_args()
    main(
        folder=args.folder,
        start=args.start,
        finish=args.finish,
    )
