import Crawl


spider = Crawl.Spider()
spider.connectToMySQL("35.221.156.22", 3306, "Cindy", "tcindy12127", "Future_db", "utf8")
spider.insertFutureInfo()
spider.insertVarietyInfo()
spider.quit()

