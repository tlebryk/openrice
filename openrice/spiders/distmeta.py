import scrapy
import pandas as pd

class DistmetaSpider(scrapy.Spider):
    name = 'distmeta'
    allowed_domains = ['openrice.com']
    start_urls = ['https://www.openrice.com/en/hongkong/restaurants']

    def parse(self, response):
        head = response.xpath("*//div[contains(@id, 'location')]")
        lis = head.css("li.dropdown-section-content-container-options-item.js-dropdown-section-content-container-options-item.js-filter-tag-item")
        lis2 = []
        for li in lis:
            # only get district data
            # there ais also location data on hotel, mall, station, 
            if li.css("::attr(data-filter)").extract() != ["district"]:
                continue
            lis2.append((lis.css("::attr(data-tag)").extract(), lis.css("::attr(data-count)").extract()))
        df = pd.DataFrame(lis2, columns=["district_name", "restaruant_count"])