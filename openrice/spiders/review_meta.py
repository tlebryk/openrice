import scrapy

class Meta(scrapy.Spider):
    name = "Meta"
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
        meta = {}
        meta['mean_rating'] = response.css(".header-score::text").get()
        meta['bookmark_count'] = response.css(".header-bookmark-count::text").get()       
        meta['district'] = response.css(".header-poi-district a::text").get()
        meta['price_range'] = response.css(".header-poi-price a::text").get()
        ratingsls = response.css(".header-score-details-right-item-range::attr(class)").getall()
        fn = lambda x: x.split(" ")[-1].replace("common_rating","").replace("_red_s","")
        ratingsls = [fn(x) for x in ratingsls]
        meta['mean_taste'] = ratingsls[0]
        meta['mean_decor'] = ratingsls[1]
        meta['mean_service'] = ratingsls[2]
        meta['mean_hygiene'] = ratingsls[3]
        meta['mean_value'] = ratingsls[4]
        meta['categories'] = response.css('.header-poi-categories a::text').getall()
        smiley = response.css(".header-smile-section").css(".score-div::text").getall()
        meta['positive_reviews'] = int(smiley[0])
        meta['ok_reviews'] = int(smiley[1])
        meta['negative_reviews'] = int(smiley[2])
        meta['total_reviews'] = meta['positive_reviews'] + meta['ok_reviews'] + meta['negative_reviews']
        return meta