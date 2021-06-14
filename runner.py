import os
import pandas as pd
from scrapy.crawler import CrawlerRunner
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from openrice.spiders.review_meta import Meta
from twisted.internet import reactor, defer
import argparse
import logging
from datetime import datetime

HOMEDIR = os.path.expanduser("~")
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/logs/openspider_{DATETIMENOW}.log",
    level=logging.DEBUG,
)




# @defer.inlineCallbacks
def get_pg(p, direct, runner, start, finish):
    logging.info(f"Working on {direct}")
    path = os.path.join(p, direct, "csvs")
    # for f in os.listdir(path):
    logging.info(f"opening file {direct}")
    df = pd.read_csv(f"{path}/{direct}.csv", encoding="utf-8")
    # NOTE: doing mini version to start
    start_len = len(df)
    
    df = df[df["OpenRice"].fillna("").str.contains("openrice")]
    ending_len = len(df)
    logging.info(f"{start_len} total raw shops, {ending_len} shops with openrice urls; \
        dropped {start_len - ending_len} without openrice info")
    df = df.sort_values("id")
    df = df.iloc[start:finish]
    urls = list(df['OpenRice'])
    ids = list(df['id'])
    runner.crawl(Meta, start_urls=urls, ids=ids, name=direct)
    runner.start()
    # reactor.stop()


def main(folder, start, finish):
    d = f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/data/"
    settings = get_project_settings()
    feed = f"{d}shop_meta/{folder}"
    if not os.path.exists(feed):
        os.makedirs(feed)
    settings["FEEDS"] = {
            f"data/shop_meta/{folder}/{folder}_{start}_{finish}.csv": {
            "format": "csv",
            "encoding": "utf8",
            "overwrite": True,
            }
    }
    # runner = CrawlerRunner(settings)
    runner = CrawlerProcess(settings=settings)
    p = os.path.join(d, "raw")
    if not folder:
        for direct in os.listdir(p):
            if os.path.isfile(f"{p}{direct}"):
                continue
            else:
                get_pg(p, direct, runner, start, finish)
    else:
        logging.info(f"Working on {folder}")
        # path = os.path.join(p, folder, "csvs")
        get_pg(p, folder, runner, start, finish) 
    # runner.start()
    # reactor.run()
    

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
        finish=args.finish
    )
