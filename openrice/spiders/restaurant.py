import scrapy

class OpenriceSpider(scrapy.Spider):
    name = "australia"
    start_urls = ["https://www.openrice.com/en/hongkong/r-australia-dairy-company-jordan-hong-kong-style-dessert-r90/reviews"]

    headers = {
            'accept-encoding': 'gzip, deflate, sdch, br',
            'accept-language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'cache-control': 'max-age=0',
        }
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    # TODO: include pagination
    def parse(self, response):
        reviews =response.css(".sr2-review-list-container")
        for review in reviews:
            head =  review.css(".middle-col").css(".review-title").xpath(".//a/@href").get()
            head = response.urljoin(head)
            yield response.follow(url=head, callback=self.review_parse, headers = self.headers)

    def review_parse(self, response):
            review_dct = {}
            review = response.css('.sr2-review-list-container')
            # information about reviewer
            reviewer_sect = review.css(".left-col")
            review_dct['total_reviews'] = reviewer_sect.css(".no-reviews::text").get()
            review_dct['a_plus'] = reviewer_sect.css(".no-editor-choice::text").get()
            # reviewer is placeholder, won't go in dictionary
            reviewer = reviewer_sect.xpath(".//a[contains(@itemprop, 'author')]")
            review_dct['username'] = reviewer.css("::text").get()
            review_dct['grade'] = reviewer_sect.css(".grade-name::text").get()
            # NOTE: must add url to base a url (stars /en/restaruant...)
            review_dct['user_link'] = reviewer.xpath(".//@href").get()

            # information about review
            review_dct['emoji'] = review.css(".left-header").xpath(".//div/@class").get()        
            # format: 2021-03-15
            review_dct['date'] = review.css(".middle-header").xpath(".//span[contains(@itemprop, 'datepublished')]/text()").get()
            #format 324 views
            review_dct['viewcount'] = review.css("span.view-count::text").get()
            review_dct['body'] = review.css("section.review-container::text").getall()

            details = review.css(".info-section")
            details_dct = {}
            # first contains date of visit, waiting time etc. 
            for detail in details[0].css("section.info"):
                key = detail.css(".title::text").get()
                value = detail.css(".text::text").get()
                details_dct[key] = value
            # second contains recommended dishs
            if len(details) > 1:
                key = details[1].css(".title::text").get()
                value = details[1].css(".dish-name::text").getall()
                details_dct[key] = value

            rate_dct = {}
            ratings = review.css(".subject")
            for rating in ratings:
                key = rating.css(".name::text").get()
                grey = len(rating.xpath(".//*[contains(@class, 'common_greystar_desktop')]"))
                yellow = len(rating.xpath(".//*[contains(@class, 'common_yellowstar_desktop')]"))
                if grey+yellow != 5:
                    self.logging.warning(f"Stars don't match: grey: {grey} \n yellow: {yellow}")
                rate_dct[key] = yellow

            review_dct.update(rate_dct)
            review_dct.update(details_dct)
            return review_dct

    # TODO: check scrapy follow syntax
    def paginate(self, response):
        pages = response.css(".pagination a::attr(href)").getall()
        for page in pages:
            yield page.follow(callback=self.review_parse)




            # 'sr2-review-list2-detailed-rating-section detail'
            # star_sect = review.xpath('sr2-review-list2-detailed-rating-section')
            
    # review = "sr2-review-list-container full clearfix js-sr2-review-list-container"