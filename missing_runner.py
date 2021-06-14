import os
import pandas as pd
from scrapy.crawler import CrawlerRunner
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from openrice.spiders.review_meta import Meta
from twisted.internet import reactor, defer
import argparse
import logging
from datetime import date, datetime

HOMEDIR = os.path.expanduser("~")
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")
DATENOW = datetime.now().strftime("%Y%m%d")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/logs/openspider_{DATETIMENOW}.log",
    level=logging.DEBUG,
)


    # p2 = os.path.join(path, direct, "csvs")


# @defer.inlineCallbacks
def get_pg(path, folder, runner, start, finish):
    """
    :path: path to directory with existing data
    :folder: basically search_string (yellow_eat, blue_shop, green_chains etc.)
    :runner: runner with desired settings
    :start: first id to start scraping with (legacy from main scraper)
    :finish: last id to stop scrapping at (legacy from main scraper)
    """
    logging.info(f"Working on {folder}")
    logging.info(f"opening file {folder}")
    df = pd.read_csv(f"{path}/{folder}.csv", encoding="utf-8")
    # NOTE: doing mini version to start
    start_len = len(df)
    df = df[df["OpenRice"].fillna("").str.contains("openrice")]
    ending_len = len(df)
    logging.info(f"{start_len} total missing shops, {ending_len} missing shops with openrice urls; \
        dropped {start_len - ending_len} without openrice info")
    df = df.sort_values("id")
    df = df.iloc[start:finish]
    urls = list(df['OpenRice'])
    logging.info(f"urls: {urls}")
    ids = list(df['id'])
    runner.crawl(Meta, start_urls=urls, ids=ids, name=folder)
    runner.start()
    # reactor.stop()


def main(folder, start, finish):
    d = f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/data/"
    settings = get_project_settings()
    feed = f"{d}shop_meta/{folder}"
    if not os.path.exists(feed):
        os.makedirs(feed)
    settings["FEEDS"] = {
            f"data/shop_meta/{folder}_missing{DATENOW}_{start}_{finish}.csv": {
            "format": "csv",
            "encoding": "utf8",
            "overwrite": True,
            }
    }
    # runner = CrawlerRunner(settings)
    runner = CrawlerProcess(settings=settings)
    # path to folder with all missing entries from first scrape
    path = os.path.join(d, "missing")
    if not folder:
        for direct in os.listdir(path):
            if os.path.isfile(f"{path}{direct}"):
                continue
            else:
                get_pg(path, direct, runner, start, finish)
    else:
        logging.info(f"Working on {folder}")
        # path = os.path.join(p, folder, "csvs")
        get_pg(path, folder, runner, start, finish) 
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
