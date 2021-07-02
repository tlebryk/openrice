import scrapy
import logging

logger = logging.getLogger('root')

class OpenriceSpider(scrapy.Spider):
    name = "ReviewSpider"
    start_urls = ["https://www.openrice.com/en/hongkong/r-green-river-restuarant-quarry-bay-hong-kong-style-r47914/reviews",
    "https://www.openrice.com/en/hongkong/r-green-river-restuarant-sham-shui-po-hong-kong-style-r27207/reviews"
    ]

    def __init__(self, df, name, **kwargs):
        self.urls_ids = zip(df["id"], df["reviews_url"])

        super().__init__(name=name, **kwargs)

    def start_requests(self):
        logging.info("starting requests")
        for id_, url in self.urls_ids:
            logger.debug(f"starting scraping for original_url {url}")
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={"og_url": url, "id_": id_})
        # for url in self.start_urls:
        #     id_=1
        #     logging.info(f"scraping url:{url}/ id{id_}")
        #     yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(og_url=url, id_ = id_))

    # TODO: include pagination
    def parse(self, response, og_url, id_):
        logging.info(f"parsing url:{og_url}/ id{id_}")

        reviews = response.css(".sr2-review-list-container")
        counter = 0
        for review in reviews:
            logging.info(f"calling reviewparse {counter} for {og_url}")
            yield self.review_parse(review, og_url=og_url, id_=id_, current_url=response.url)
            counter+=1
        next_page = self.paginate(response)
        if next_page:
            logging.info(f"Going to next page {next_page}")
            yield response.follow(url=next_page, callback=self.parse, cb_kwargs=dict(id_=id_, og_url=og_url))
        else:
            logging.info(f"url {og_url} could not find next page. Closing...")

        # for page in self.paginate(response):
        #     yield page
        
        # page_links = self.page_parse(response)
        # yield from response.follow_all(page_links, self.review_parse)
        # for article in  self.page_parse(response):
        #     yield response.follow_allarticle

    def page_parse(self, response):
        reviews = response.css(".sr2-review-list-container")
        for review in reviews:
            yield self.review_parse(review)
            head =  review.css(".middle-col").css(".review-title").xpath(".//a/@href").get()
            head = response.urljoin(head)
            yield response.follow(url=head, callback=self.review_parse)
        response.css(".pagination-button")

    def review_parse(self, review, og_url, id_, current_url):
            logging.info("calling review parse...")
            review_dct = {
                "url": og_url,
                "id": id_,
            }
            rev = review.css('.sr2-review-list-container')
            # information about reviewer
            reviewer_sect = rev.css(".left-col")
            review_dct['total_reviews'] = reviewer_sect.css(".no-reviews::text").get()
            review_dct["final_url"] = current_url
            review_dct['a_plus'] = reviewer_sect.css(".no-editor-choice::text").get()
            # reviewer is placeholder, won't go in dictionary
            reviewer = reviewer_sect.xpath(".//a[contains(@itemprop, 'author')]")
            review_dct['username'] = reviewer.css("::text").get()
            review_dct['grade'] = reviewer_sect.css(".grade-name::text").get()
            # NOTE: must add url to base a url (stars /en/restaruant...)
            review_dct['user_link'] = reviewer.xpath(".//@href").get()
            # information about review
            title = rev.css("div.review-title")
            review_dct["title"] = title.css("*::text").get()
            review_dct["review_link"] = title.xpath(".//a/@href").get()


            review_dct['emoji'] = rev.css(".left-header").xpath(".//div/@class").get()        
            # format: 2021-03-15
            review_dct['date'] = rev.css(".middle-header").xpath(".//span[contains(@itemprop, 'datepublished')]/text()").get()
            #format 324 views
            review_dct['viewcount'] = rev.css("span.view-count::text").get()
            body = rev.css("section.review-container::text").getall()
            body = "\n".join([b.strip() for b in body])
            review_dct['body'] = body
            details = rev.css(".info-section")
            details_dct = {}
            # first contains date of visit, waiting time etc. 
            if details_dct:
                for detail in details[0].css("section.info"):
                    key = detail.css(".title::text").get().replace(" ", "_").lower()
                    value = detail.css(".text::text").get()
                    details_dct[key] = value
                # second contains recommended dishs
                if len(details) > 1:
                    key = details[1].css(".title::text").get().replace(" ", "_").lower()
                    value = details[1].css(".dish-name::text").getall()
                    details_dct[key] = value
            fields = ["date_of_visit", "dining_method", "spending_per_head", "recommended_dishes", "waiting_time", "type_of_meal"]
            for f in fields:
                if not details_dct.get(f):
                    details_dct[f] = None
            review_dct.update(details_dct)

            rate_dct = {}
            ratings = rev.css(".subject")
            for rating in ratings:
                key = rating.css(".name::text").get().replace(" ", "_").lower()
                grey = len(rating.xpath(".//*[contains(@class, 'common_greystar_desktop')]"))
                yellow = len(rating.xpath(".//*[contains(@class, 'common_yellowstar_desktop')]"))
                if grey+yellow != 5:
                    self.logging.warning(f"Stars don't match: grey: {grey} \n yellow: {yellow}")
                rate_dct[key] = yellow
            rate_fields = ["taste", "decor", "service", "hygiene", "value"]
            for f in rate_fields:
                if not details_dct.get(f):
                    details_dct[f] = None
            review_dct.update(rate_dct)
            return review_dct

    # TODO: check scrapy follow syntax
    def paginate(self, response):
        next_page = response.css(".pagination-button.next.js-next::attr(href)").get()
        logging.info(f"Got next page: {next_page}")
        return next_page
        # pages = response.css(".pagination-button a::attr(href)").getall()
        # for page in pages:
        #     yield response.follow(page, callback=self.page_parse)