import datetime


def serialize(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat(timespec="milliseconds")
    if isinstance(obj, datetime.date):
        return str(obj)
    return obj
