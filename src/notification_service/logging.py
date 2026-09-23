import json
import logging


def event(name, **fields):
    logging.getLogger("notification").info(json.dumps({"event": name, **fields}, ensure_ascii=False, default=str))


def configure():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)

