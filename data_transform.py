import datetime
import json

class DateTimeEncoder(json.JSONEncoder):
    """Transforms datetime objects into isoformat so that they are JSON serialisable"""
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)