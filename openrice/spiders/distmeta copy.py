import scrapy
import pandas as pd
from scrapy.http import request
from datetime import datetime

# flexible columns therefore use json feed

class DistmetaSpider(scrapy.Spider):
    name = 'cuisinetype'
    allowed_domains = ['openrice.com']
    start_urls = ['https://www.openrice.com/en/hongkong/restaurants']
    base_url = "https://www.openrice.com/en/hongkong/restaurants/district/"
    def start_requests(self):
        df = pd.read_csv(r"C:\Users\tlebr\OneDrive - pku.edu.cn\China in Transition\openrice\data\district\ORdistdata.csv", 
            encoding="utf-8")
        df["dist_name_url_compat"] = df.district_name.str.replace(" ", "-")
        # for el in df.dist_name_url_compat:
        # count  = 0
        for index, row in df.iterrows():
            url = self.base_url + row.dist_name_url_compat
            yield scrapy.Request(url=url, callback=self.parse, 
                cb_kwargs={ 
                    "index": index,
                    "district_name": row["district_name"],
                })
            # count += 1
            # if count > 2:
            #     break

    def parse(self, response, index, district_name):
        dct = {
            "index": index,
            "district_name": district_name,
            "scrape_time": datetime.now(),
        }
        head = response.xpath("*//div[contains(@id, 'cuisineFilter')]")
        lis = head.css("li.dropdown-section-content-container-options-item.js-dropdown-section-content-container-options-item.js-filter-tag-item")
        lis2 = []
        for li in lis:
            # only get district data
            # there ais also location data on hotel, mall, station, 
            # if li.css("::attr(data-filter)").extract() != ["district"]:
            #     continue
            # 
            lis2.append( ("cuisine_" + (li.css("::attr(data-tag)").extract()[0]).replace(" ", "_"), 
                li.css("::attr(data-count)").extract()[0]))
        df = pd.DataFrame(lis2, columns=["cuisine_name", "restaruant_count"])
        df = df.set_index("cuisine_name")
        dct.update(df.to_dict()["restaruant_count"])

        head = response.xpath("*//div[contains(@id, 'dishFilter')]")
        lis = head.css("li.dropdown-section-content-container-options-item.js-dropdown-section-content-container-options-item.js-filter-tag-item")
        lis2 = []
        for li in lis:
            # only get district data
            # there ais also location data on hotel, mall, station, 
            # if li.css("::attr(data-filter)").extract() != ["district"]:
            #     continue
            lis2.append(("dish_" + (li.css("::attr(data-tag)").extract()[0]).replace(" ", "_"),
                li.css("::attr(data-count)").extract()[0]))
            df = pd.DataFrame(lis2, columns=["dish_name", "restaruant_count"])
            df = df.set_index("dish_name")
            dct.update(df.to_dict()["restaruant_count"])
        return dct
        # df.to_csv(r"data\dish\ORcuisinedata.csv", index=False, encoding="utf-8")