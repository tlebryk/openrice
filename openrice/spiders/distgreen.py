import scrapy
import pandas as pd
from scrapy.http import request
from datetime import datetime
import logging
import os
import sys
from html_text import extract_text

CURRENTDIR = os.getcwd()
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")
LOGPATH = f"{CURRENTDIR}/logs/{os.path.basename(__file__)[:-3]}/"
if not os.path.exists(LOGPATH):
    os.makedirs(LOGPATH)
logging.basicConfig(
    filename=f"{LOGPATH}/{DATETIMENOW}.log",
    format="%(asctime)s %(levelname)-8s  %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class DistmetaSpider(scrapy.Spider):
    name = "distgreen"
    allowed_domains = ["openrice.com"]
    start_urls = ["https://www.openrice.com/en/hongkong/restaurants"]
    base_url = "https://www.openrice.com/en/hongkong/restaurants/district/"

    def start_requests(self):
        df = pd.read_csv(
            r"C:\Users\tlebr\OneDrive - pku.edu.cn\China in Transition\openrice\data\district\ORdistdata.csv",
            encoding="utf-8",
        )
        df["dist_name_url_compat"] = df.district_name.str.replace(" ", "-").str.lower()
        counter = 0
        for index, row in df.iterrows():
            url = self.base_url + row.dist_name_url_compat
            logging.info(f"fetching {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                cb_kwargs={
                    "index": index,
                    "district_name": row["district_name"],
                },
            )
            counter += 1
            if counter > 1:
                break

    def paginate(self, response):
        next_page = response.css(".pagination-button.next.js-next::attr(href)").get()
        logging.info(f"Got next page: {next_page}")
        return next_page

    def parse(self, response, index, district_name):
        restaurants = response.css(
            "li.sr1-listing-content-cell.pois-restaurant-list-cell"
        )
        for rest in restaurants:
            h2 = rest.css("h2.title-name")
            dct = dict(
                reviews_url="https://www.openrice.com/" + h2.xpath(".//a/@href").get(),
                title=extract_text(h2.css("a").get()),
                district=district_name,
                index=index,
                parent_url=response.url,
                address=extract_text(rest.css("div.icon-info.address").get()),
                price_range=extract_text(
                    rest.css("div.icon-info.icon-info-food-price").get()
                ),
                cuisine=extract_text(
                    rest.css("div.icon-info.icon-info-food-name").get()
                ),
                positive_reviews=extract_text(
                    rest.css(
                        "div.emoticon-container.smile-face.pois-restaurant-list-cell-content-right-info-rating-happy"
                    ).get()
                ),
                negative_reviews=extract_text(
                    rest.css(
                        "div.emoticon-container.sad-face.pois-restaurant-list-cell-content-right-info-rating-sad"
                    ).get()
                ),
                bookmark_count=rest.css(
                    "div.text.bookmarkedUserCount.js-bookmark-count::attr(data-count)"
                ).get(),
                total_reviews=extract_text(rest.css("div.counters-container").get()),
                promo=rest.css("img.promotion-logo-image::attr(src)").get(),
            )
            yield dct
        next_page = self.paginate(response)
        if next_page:
            logging.info(f"Going to next page {next_page}")
            yield response.follow(
                url=next_page,
                callback=self.parse,
                cb_kwargs=dict(index=index, district_name=district_name),
            )
        else:
            logging.info(f"url {response.url} could not find next page. Closing...")