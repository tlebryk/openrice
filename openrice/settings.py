from openrice.middlewares import RedirectMiddleware
import os
from datetime import datetime
import logging

# Scrapy settings for openrice project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "openrice"

SPIDER_MODULES = ["openrice.spiders"]
NEWSPIDER_MODULE = "openrice.spiders"


HOMEDIR = os.path.expanduser("~")
DATETIMENOW = datetime.now().strftime("%Y%m%d_%H%M%S")

# LOG_STDOUT = True
# LOG_FILE = f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/logs/openspider_{DATETIMENOW}.log"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'openrice (+http://www.yourdomain.com)'

# Obey robots.txt rules
# ROBOTSTXT_OBEY = True


DEFAULT_REQUEST_HEADERS = {
    "accept-encoding": "gzip, deflate, sdch, br",
    "accept-language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4",
    "upgrade-insecure-requests": "1",
    # 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "cache-control": "max-age=0",
}
# TODO: UNCOMMENT
# PROXY_POOL_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    "openrice.middlewares.ProxyPoolMiddleware": 610,
    "openrice.middlewares.BanDetectionMiddleware": 620,
    "scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware": None,
    # TODO: UNCOMMENT
    "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 800,
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    # 'scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware': 500,
    # TODO: UNCOMMENT
    "scrapy.contrib.downloadermiddleware.redirect.RedirectMiddleware": None,
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": None,
    "scrapy.contrib.downloadermiddleware.redirect.MetaRefreshMiddleware": None,
    "openrice.middlewares.RedirectMiddleware": 810,
}

PROXY_POOL_BAN_POLICY = "openrice.policy.BanDetectionPolicyNotText"


# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

RANDOM_UA_PER_PROXY = True

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#     'accept-encoding': 'gzip, deflate, sdch, br',
#     'accept-language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
#     'upgrade-insecure-requests': '1',
#     # 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
#     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#     'cache-control': 'max-age=0',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'openrice.middlewares.OpenriceSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'openrice.middlewares.OpenriceDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'openrice.pipelines.OpenricePipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

USER_AGENTS = [
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/57.0.2987.110 "
        "Safari/537.36"
    ),  # chrome
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/61.0.3163.79 "
        "Safari/537.36"
    ),  # chrome
    (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) "
        "Gecko/20100101 "
        "Firefox/55.0"
    ),  # firefox
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/61.0.3163.91 "
        "Safari/537.36"
    ),  # chrome
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/62.0.3202.89 "
        "Safari/537.36"
    ),  # chrome
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/63.0.3239.108 "
        "Safari/537.36"
    ),  # chrome
]


# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
