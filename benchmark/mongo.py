"""One connection pool per process; no clients are constructed before gevent patching."""
import os

from pymongo import MongoClient, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


def connect(cfg, role="worker"):
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise ValueError("Set MONGO_URI to a MongoDB URI (never put credentials in configuration files)")
    settings = cfg["mongo"]
    client = MongoClient(
        uri, appname=f"locust-{role}-{cfg['label']}"[:128],
        maxPoolSize=settings["max_pool_size"],
        timeoutMS=settings["timeout_ms"], serverSelectionTimeoutMS=settings["timeout_ms"],
        waitQueueTimeoutMS=settings["timeout_ms"], tz_aware=True,
    )
    preferences = {"primary": ReadPreference.PRIMARY, "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED}
    preference = preferences[settings["read_preference"]]
    wc = settings["write_concern"]
    if isinstance(wc, str) and wc.isdigit():
        wc = int(wc)
    db = client.get_database(cfg["database"], read_preference=preference,
                             read_concern=ReadConcern(settings["read_concern"]),
                             write_concern=WriteConcern(w=wc))
    return client, db
