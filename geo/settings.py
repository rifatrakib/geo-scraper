import os

from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "geo"

SPIDER_MODULES = ["geo.spiders"]
NEWSPIDER_MODULE = "geo.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "geo (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 8

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 8
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#   "Accept-Language": "en",
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    "geo.middlewares.GeoSpiderMiddleware": 543,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "geo.middlewares.GeoDownloaderMiddleware": 543,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "geo.pipelines.CSVPipeline": 100,
    "geo.pipelines.JSONPipeline": 200,
    "geo.pipelines.JSONLinesPipeline": 300,
    "geo.pipelines.MongoDBPipeline": 400,
    "geo.pipelines.MetaPipeline": 500,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Downloader settings
DOWNLOAD_WARNSIZE = 1073741824
DOWNLOAD_MAXSIZE = 10737418240
DOWNLOAD_TIMEOUT = 600

# signin variables
SIGNIN_PATH = os.environ.get("SIGNIN_PATH")
EMAIL = os.environ.get("EMAIL")
PASSWORD = os.environ.get("PASSWORD")

# links to scrape
ADVERTS_COUNTER = os.environ.get("ADVERTS_COUNTER")
ADVERTS_PAGES = os.environ.get("ADVERTS_PAGES")

LANDS_COUNTER = os.environ.get("LANDS_COUNTER")
LANDS_PAGES = os.environ.get("LANDS_PAGES")

ESTABLISHMENTS_COUNTER = os.environ.get("ESTABLISHMENTS_COUNTER")
ESTABLISHMENTS_PAGES = os.environ.get("ESTABLISHMENTS_PAGES")

DEALINGS_COUNTER = os.environ.get("DEALINGS_COUNTER")
DEALINGS_PAGES = os.environ.get("DEALINGS_PAGES")

THRESHOLDS_PAGES = os.environ.get("THRESHOLDS_PAGES")
THRESHOLDS_IDENTIFIER = os.environ.get("THRESHOLDS_IDENTIFIER")
THRESHOLDS_CONNECTOR = os.environ.get("THRESHOLDS_CONNECTOR")

FLAT_INFO_URL = os.environ.get("FLAT_INFO_URL")
FLAT_INFO_PARAM = os.environ.get("FLAT_INFO_PARAM")

WARNING_URL = os.environ.get("WARNING_URL")
WARNING_PARAM = os.environ.get("WARNING_PARAM")

PURPOSE_URL = os.environ.get("PURPOSE_URL")
PURPOSE_PARAM = os.environ.get("PURPOSE_PARAM")

PLAN_URL = os.environ.get("PLAN_URL")
PLAN_PARAM = os.environ.get("PLAN_PARAM")

DWELLERS_URL = os.environ.get("DWELLERS_URL")

APARTMENT_URL = os.environ.get("APARTMENT_URL")
SUBLETERS_URL = os.environ.get("SUBLETERS_URL")

EXTERNAL_LINKS_URL = os.environ.get("EXTERNAL_LINKS_URL")
EXTERNAL_LINKS_PARAM = os.environ.get("EXTERNAL_LINKS_PARAM")

FLAT_REGISTRATION_URL = os.environ.get("FLAT_REGISTRATION_URL")
FLAT_LOCATION_URL = os.environ.get("FLAT_LOCATION_URL")

LAND_ESTABLISHMENTS_URL = os.environ.get("LAND_ESTABLISHMENTS_URL")
LAND_FLATS_URL = os.environ.get("LAND_FLATS_URL")
LAND_UTILITIES_URL = os.environ.get("LAND_UTILITIES_URL")
LAND_ATTACHMENTS_URL = os.environ.get("LAND_ATTACHMENTS_URL")
LAND_REGISTRATION_URL = os.environ.get("LAND_REGISTRATION_URL")

LAND_ADDRESS_URL = os.environ.get("LAND_ADDRESS_URL")

SURVEY_NAMES_URL = os.environ.get("SURVEY_NAMES_URL")
SURVEY_REPORT_URL = os.environ.get("SURVEY_REPORT_URL")

FLAT_PRICE_ESTIMATION_URL = os.environ.get("FLAT_PRICE_ESTIMATION_URL")
FLAT_PRICE_ESTIMATION_PARAM = os.environ.get("FLAT_PRICE_ESTIMATION_PARAM")
FLAT_SALE_ADS_URL = os.environ.get("FLAT_SALE_ADS_URL")
FLAT_SALE_ADS_PARAM = os.environ.get("FLAT_SALE_ADS_PARAM")
FLAT_BUILDING_IDENTIFIER = os.environ.get("FLAT_BUILDING_IDENTIFIER")
LAND_BUILDING_IDENTIFIER = os.environ.get("LAND_BUILDING_IDENTIFIER")

OWNER_INFORMATION_URL = os.environ.get("OWNER_INFORMATION_URL")
OWNER_PATH = os.environ.get("OWNER_PATH")

BUILDING_DETAILS_URL = os.environ.get("BUILDING_DETAILS_URL")
BUILDING_DETAILS_PARAM = os.environ.get("BUILDING_DETAILS_PARAM")

LATEST_DEALINGS_URL = os.environ.get("LATEST_DEALINGS_URL")
HISTORICAL_DEALINGS_URL = os.environ.get("HISTORICAL_DEALINGS_URL")

FLAT_EVALUATION_URL = os.environ.get("FLAT_EVALUATION_URL")
FLAT_EVALUATION_PARAM = os.environ.get("FLAT_EVALUATION_PARAM")

INDEX_ESTIMATION_URL = os.environ.get("INDEX_ESTIMATION_URL")
INDEX_ESTIMATION_PARAM = os.environ.get("INDEX_ESTIMATION_PARAM")
INDEX_ESTIMATION_SOURCE = os.environ.get("INDEX_ESTIMATION_SOURCE")

PRICE_DISTRIBUTION_URL = os.environ.get("PRICE_DISTRIBUTION_URL")
PRICE_DISTRIBUTION_PARAM = os.environ.get("PRICE_DISTRIBUTION_PARAM")
PRICE_DISTRIBUTION_SOURCE = os.environ.get("PRICE_DISTRIBUTION_SOURCE")

ADJACENT_LANDS_URL = os.environ.get("ADJACENT_LANDS_URL")
ADJACENT_LANDS_SOURCE = os.environ.get("ADJACENT_LANDS_SOURCE")

FLAT_ASSOCIATION_URL = os.environ.get("FLAT_ASSOCIATION_URL")
FLAT_UTILITIES_URL = os.environ.get("FLAT_UTILITIES_URL")
FLAT_ASSOCIATION_PARAM = os.environ.get("FLAT_ASSOCIATION_PARAM")

# database credentials
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE")

# website dynamic links
HOME_URL = os.environ.get("HOME_URL")
COUNTER_PATH = os.environ.get("COUNTER_PATH")
TABLE_PATH = os.environ.get("TABLE_PATH")
