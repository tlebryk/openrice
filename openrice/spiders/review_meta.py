import scrapy
import logging
import os
from datetime import datetime
from scrapy.utils.project import get_project_settings


HOMEDIR = os.path.expanduser("~")
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")


# logging.basicConfig(
#     format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     filename=f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/logs/openspider_{DATETIMENOW}.log",
#     level=logging.DEBUG,
# )
# logging.info("test1")


logger = logging.getLogger('root')

class Meta(scrapy.Spider):
    name = "Meta"
    # start_urls = ["https://s.openrice.com/QrbS0m54A00~d9kqaCjV2"]

    def __init__(self, start_urls, ids, name, **kwargs):
        self.start_urls = start_urls
        self.ids = ids
        super().__init__(name=name, **kwargs)


    def start_requests(self):
        urls_ids = zip(self.ids, self.start_urls)
        # for i in range(3):
            # yield dict(a=1,b=2,c=3)
        print(f'original settings: {self.settings}')
        for id_, url in urls_ids:
            og_url = url
            logging.debug(f"starting scraping for original_url {og_url}")
            url = url.replace(".com/zh/", ".com/en/")
            logging.debug(f"checking original_url unchanged {og_url}")
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={"og_url": og_url, "id_": id_})

    def get_review_links(self, response):
        x= response.css("div.show-all-reviews-btn")
        if x:
            url = x.css("a::attr(href)").get()
            url = response.urljoin(url)
            return url
        else:
            x = response.css("div.main-menu.table-center")
            lis = x.css("li")
            for li in lis:
                if li.css("* ::text").get().split()[0].strip().lower() == "review":
                    url = li.css("a::attr(href)").get()
                    url = response.urljoin(url)
                    return url

        
    def get_job_links(self, response):
        x = response.css("div.js-sr2-jobs-layer")
        if not x:
            return None
        url = x.css("a::attr(href)").get()
        url = response.urljoin(url)
        return url

    def get_num_jobs(self, response):
        x = response.css("span.jobs-counter-badge")
        if not x:
            return None
        return x.css("* ::text").get()

    def get_add_info(self, response):
        x = response.css('section.poi-additional-info-section')
        if not x:
            return None
        return x.css("div.text::text").get()

    def get_num_seats(self, response):
        x = response.css("section")
        for el in x:
            try:
                if el.css("div.title::text").get().lower().strip() == "number of seats":
                    return el.css("div.content::text").get()
            except:
                continue

    def get_payment_methods(self, response):
        x = response.css("section")
        for el in x:
            try:
                if el.css("div.title::text").get().lower().strip() == "payment methods":
                    return el.css("span::text").getall()
            except:
                continue
        return None

    def get_other_info(self, response):
        """
        # 1 = yes/check;
        # 0 = no/x;
        :returns: generator of tuples with (other_info_type, 1/0) 
        other_info_type is usually "wifi, alchoholic drinks, delivery, 10% service fee, tv broadcast etc.)
        """
        x = response.css("section.conditions-section")
        conditions = x.css("div.condition-item")
        for condition in conditions:
            text = condition.css("span.condition-name::text").get()
            text = text.replace(" ", "_")
            tickcross = condition.css("span.or-sprite-inline-block::attr(class)").get()
            tickcross = tickcross.split(" ")[-1]
            if "tick" in tickcross:
                tickcross = 1
            elif "cross" in tickcross:
                tickcross = 0
            yield (text, tickcross)

    # TODO: include pagination
    def parse(self, response, og_url, id_):
        # return dict(a=1,b=2,c=3)
        meta = {}
        meta["id"] = id_
        meta["original_url"] = og_url
        meta["response_url"] = response.url
        meta["reviews_url"] = self.get_review_links(response)
        meta["jobs_link"] = self.get_job_links(response)
        meta["num_jobs"] = self.get_num_jobs(response)
        meta["additional_info"] = self.get_add_info(response)
        meta["number_of_seats"] = self.get_num_seats(response)
        meta['mean_rating'] = response.css(".header-score::text").get()
        meta['bookmark_count'] = response.css(".header-bookmark-count::text").get()       
        meta['district'] = response.css(".header-poi-district a::text").get()
        meta['price_range'] = response.css(".header-poi-price a::text").get()
        ratingsls = response.css(".header-score-details-right-item-range::attr(class)").getall()
        fn = lambda x: x.split(" ")[-1].replace("common_rating","").replace("_red_s","")
        ratingsls = [fn(x) for x in ratingsls]
        if ratingsls:
            meta['mean_taste'] = ratingsls[0]
            meta['mean_decor'] = ratingsls[1]
            meta['mean_service'] = ratingsls[2]
            meta['mean_hygiene'] = ratingsls[3]
            meta['mean_value'] = ratingsls[4]
        categories = response.css('.header-poi-categories a::text').getall()
        for i, category in enumerate(categories):
            meta[f'categories_{i}'] = category
        smiley = response.css(".header-smile-section").css(".score-div::text").getall()
        meta['positive_reviews'] = int(smiley[0])
        meta['ok_reviews'] = int(smiley[1])
        meta['negative_reviews'] = int(smiley[2])
        meta['total_reviews'] = meta['positive_reviews'] + meta['ok_reviews'] + meta['negative_reviews']
        payment_methods = self.get_payment_methods(response)
        if payment_methods:
            for i, method in enumerate(payment_methods):
                meta[f"payment_method_{i}"] = method
        other_info = self.get_other_info(response)
        if other_info:
            for text, class_ in other_info:
                meta[f"other_{text}"] = class_
        return meta