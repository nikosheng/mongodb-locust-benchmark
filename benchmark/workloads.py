import random
import time
from datetime import datetime, timezone

from bson import Int64
from bson.decimal128 import Decimal128
from pymongo import UpdateOne

from benchmark.adt import pipeline
from benchmark.config import as_of
from generate_member_profile import member_no_of


class Workloads:
    def __init__(self, cfg, db, events, seed, run_id):
        self.cfg, self.db, self.events = cfg, db, events
        self.rng = random.Random(seed)
        self.run_id = run_id
        self.profiles = db[cfg["profiles"]]
        self.daily = db[cfg["daily"]]
        self.identities = db[cfg["identities"]]
        self.operations = list(cfg["profile_weights"])
        self.weights = list(cfg["profile_weights"].values())

    def measured(self, name, function, request_type="MONGO"):
        start = time.perf_counter()
        error = None
        try:
            return function()
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.events.request.fire(request_type=request_type, name=name,
                                     response_time=(time.perf_counter() - start) * 1000,
                                     response_length=0, exception=error)

    def player(self):
        cfg = self.cfg
        count = cfg["players"]
        if cfg["distribution"] == "hotspot":
            index = 0
        elif cfg["distribution"] == "skewed" and self.rng.random() < cfg["hot_probability"]:
            index = self.rng.randrange(max(1, int(count * cfg["hot_fraction"])))
        else:
            index = self.rng.randrange(count)
        return Int64(cfg["player_id_base"] + index)

    @staticmethod
    def require_profile(document):
        if document is None:
            raise ValueError("Expected profile missing; verify dataset/configuration")
        return document

    def update(self, pid):
        result = self.profiles.update_one({"player_id": pid}, {"$set": {
            "pinfo.nick_name": f"bench-{self.rng.randrange(100000)}",
            "_ODS_modified_dtm": datetime.now(timezone.utc)}})
        if result.matched_count != 1:
            raise ValueError("Profile update matched no document")

    def profile(self):
        pid = self.player()
        op = self.rng.choices(self.operations, self.weights)[0]

        def execute():
            if op in {"read_full", "read_projected"}:
                projection = None if op == "read_full" else {"player_id": 1, "club_state_level_id": 1, "adt": 1}
                return self.require_profile(self.profiles.find_one({"player_id": pid}, projection))
            if op == "update_contact":
                return self.update(pid)
            if op == "update_bucket":
                result = self.profiles.update_one({"player_id": pid}, {"$set": {
                    "bucket_bal.$[b].amount": Decimal128(f"{self.rng.randrange(1000000) / 100:.2f}"),
                    "_ODS_modified_dtm": datetime.now(timezone.utc)}}, array_filters=[{"b.group_id": 1}])
                if result.matched_count != 1:
                    raise ValueError("Bucket update matched no document")
            elif op == "read_update":
                self.require_profile(self.profiles.find_one({"player_id": pid}))
                self.update(pid)
            elif op == "member_lookup":
                return self.require_profile(self.profiles.find_one({"member_no": member_no_of(int(pid) - self.cfg["player_id_base"])}))
            elif op == "card_lookup":
                # The reference schema intentionally has ~5% of patrons without a card.
                return self.profiles.find_one({"member_card.card_num": member_no_of(int(pid) - self.cfg["player_id_base"])})
            elif op == "host_list":
                return list(self.profiles.find({"primary_host_num": f"E{self.rng.randrange(5001, 5301)}", "is_vip": True},
                                              {"player_id": 1, "club_state_level_id": 1}).sort("player_id", 1).limit(50))
            elif op == "identity_journey":
                key = "member:" + member_no_of(int(pid) - self.cfg["player_id_base"])
                identity = self.require_profile(self.identities.find_one({"lookup_key": key}))
                return self.require_profile(self.profiles.find_one({"player_id": identity["player_id"]}))

        return self.measured(f"profile.{op}", execute)

    def compute(self, lo=None, hi=None, ids=None):
        cfg = self.cfg
        if ids is None and cfg["batch"]["mode"] == "ids":
            ids = range(lo, hi)
        pipe = pipeline(cfg, ids=ids, bounds=(lo, hi))
        rows = self.measured("adt.compute_batch", lambda: list(self.daily.aggregate(
            pipe, allowDiskUse=True, batchSize=cfg["batch"]["size"], comment=f"bench:{self.run_id}")))
        counts = {"patrons": len(rows), "daily_rows": sum(
            r["adt"]["active_days"][f"d{max(cfg['windows'])}"] for r in rows), "profiles_written": 0}
        if rows and cfg["batch"]["writeback"]:
            now = datetime.now(timezone.utc)
            operations = []
            for row in rows:
                row["adt"].update(as_of=as_of(cfg), computed_at=now, run_id=self.run_id)
                operations.append(UpdateOne({"player_id": row["_id"]}, {"$set": {
                    "adt": row["adt"], "_ODS_modified_dtm": now}}))

            def write():
                result = self.profiles.bulk_write(operations, ordered=False)
                if result.matched_count != len(operations):
                    raise ValueError("ADT writeback matched fewer profiles than expected")
                return result.matched_count

            counts["profiles_written"] = self.measured("adt.writeback_batch", write)
        return counts

    def incremental(self):
        # E2E is the business metric; component MONGO events are diagnostic only.
        return self.measured("adt.refresh_patron_e2e", lambda: self.compute(ids=[self.player()]), "BUSINESS")

    def ingest(self):
        # Fixed patron-day key makes retries idempotent and keeps dataset size bounded.
        # Uses the next day, outside the immutable ADT benchmark window.
        from datetime import timedelta
        pid = self.player()
        date = as_of(self.cfg) + timedelta(days=1)
        return self.measured("gaming.upsert_patron_day", lambda: self.daily.update_one(
            {"player_id": pid, "gaming_date": date}, {"$set": {
                "gaming.total.theo_win": Decimal128(f"{self.rng.randrange(100000) / 100:.2f}"),
                "_ODS_modified_dtm": datetime.now(timezone.utc)},
                "$setOnInsert": {"_id": f"{pid}:{date:%Y-%m-%d}"}}, upsert=True))

    def online(self):
        return {"incremental": self.incremental, "ingest": self.ingest}.get(self.cfg["scenario"], self.profile)()
