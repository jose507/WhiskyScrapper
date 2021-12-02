import pymongo



from scrapy.exceptions import DropItem
from scrapy.settings import Settings


class WiskyscraperPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            'localhost',
            27017
        )
        db = connection["rest_db"]
        self.collection = db["wiskeys"]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert(dict(item))
            print("YEYO ////////////")
        return item
