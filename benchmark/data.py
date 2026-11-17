"""Deterministic data preparation and post-run correctness/routing checks."""
import argparse
import hashlib
import json
import random
from datetime import timedelta

from bson import Int64, json_util
from bson.decimal128 import Decimal128
from pymongo import ReplaceOne

from benchmark.adt import pipeline, reference
from benchmark.config import as_of, load_config
from benchmark.mongo import connect
from generate_member_profile import DEFAULT_INDEXES, gen_doc, member_no_of


def profile(cfg, index):
    if cfg["player_id_base"] != 10000000:
        raise ValueError("Reference profile generator requires player_id_base=10000000")
    doc = gen_doc(random.Random(cfg["seed"] * 1000000007 + index), index)
    doc["_id"] = f"player:{doc['player_id']}"
    return doc


def daily_rows(cfg, index):
    rng = random.Random(cfg["seed"] * 1000000007 + index + 999999937)
    pid = Int64(cfg["player_id_base"] + index)
    for day in range(cfg["history_days"]):
        if rng.random() >= cfg["active_day_probability"]:
            continue
        date = as_of(cfg) - timedelta(days=day)
        cents = rng.randrange(100, 100000)
        yield {"_id": f"{pid}:{date:%Y-%m-%d}", "player_id": pid, "gaming_date": date,
               "gaming": {"total": {"theo_win": Decimal128(f"{cents / 100:.2f}"),
                                    "actual_win": Decimal128(f"{rng.randrange(-100000, 100000) / 100:.2f}"),
                                    "turnover": Decimal128(f"{cents * 20 / 100:.2f}")}},
               "_ODS_modified_dtm": date}


def setup(client, db, cfg, sharded, identities):
    if sharded and client.admin.command("hello").get("msg") != "isdbgrid":
        raise ValueError("--sharded requires a mongos connection")
    expected_shards = cfg.get("shards", 1)
    if sharded and expected_shards == 2:
        actual_shards = len(client.admin.command("listShards")["shards"])
        if actual_shards != expected_shards:
            raise ValueError(
                f"The config expects {expected_shards} shards, but mongos reports {actual_shards}; "
                "connect to the intended two-shard cluster before setup"
            )
    collections = [(cfg["profiles"], "player_id"), (cfg["daily"], "player_id")]
    if identities:
        collections.append((cfg["identities"], "lookup_key"))
    for name, key in collections:
        if name not in db.list_collection_names():
            db.create_collection(name)
        db[name].create_index([(key, "hashed")])  # equivalent index cost for unsharded baseline
        if sharded:
            namespace = f"{cfg['database']}.{name}"
            command = {"shardCollection": namespace, "key": {key: "hashed"}}
            if expected_shards == 2:
                # Pre-split an empty hashed collection so both shards receive chunks
                # before the loader starts inserting documents.
                command["numInitialChunks"] = 4
            # numInitialChunks only applies when initially sharding an empty collection.
            client.admin.command(command)
    for spec in DEFAULT_INDEXES:
        db[cfg["profiles"]].create_index(list(spec.items()))
    db[cfg["profiles"]].create_index([("primary_host_num", 1), ("is_vip", 1), ("player_id", 1)])
    db[cfg["daily"]].create_index([("player_id", 1), ("gaming_date", 1)])
    if identities:
        db[cfg["identities"]].create_index("lookup_key")


def dataset_spec(cfg):
    keys = ("seed", "players", "player_id_base", "history_days", "active_day_probability", "as_of")
    return {k: cfg[k] for k in keys}


def load(db, cfg, partition, partitions, batch_size, identities):
    profile_ops, daily_ops, identity_ops = [], [], []
    total_profiles = total_daily = 0

    def flush(collection, operations):
        if operations:
            db[collection].bulk_write(operations, ordered=False)
            operations.clear()

    for index in range(partition, cfg["players"], partitions):
        doc = profile(cfg, index)
        profile_ops.append(ReplaceOne({"player_id": doc["player_id"]}, doc, upsert=True))
        total_profiles += 1
        if identities:
            key = "member:" + member_no_of(index)
            identity_ops.append(ReplaceOne({"lookup_key": key}, {
                "_id": key, "lookup_key": key, "player_id": doc["player_id"]}, upsert=True))
        for row in daily_rows(cfg, index):
            daily_ops.append(ReplaceOne({"player_id": row["player_id"], "gaming_date": row["gaming_date"]}, row, upsert=True))
            total_daily += 1
            if len(daily_ops) >= batch_size:
                flush(cfg["daily"], daily_ops)
        if len(profile_ops) >= batch_size:
            flush(cfg["profiles"], profile_ops)
            flush(cfg["identities"], identity_ops)
            print(f"partition={partition} profiles={total_profiles} daily={total_daily}", flush=True)
    flush(cfg["profiles"], profile_ops)
    flush(cfg["daily"], daily_ops)
    flush(cfg["identities"], identity_ops)
    spec = dataset_spec(cfg)
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    db["benchmark_loads"].replace_one({"_id": f"{digest}:{partition}"}, {
        "_id": f"{digest}:{partition}", "dataset": spec, "partitions": partitions,
        "partition": partition, "profiles": total_profiles, "daily_rows": total_daily}, upsert=True)
    print(json.dumps({"partition": partition, "profiles": total_profiles, "daily_rows": total_daily}))


def validate(db, cfg, samples, run_id):
    rng = random.Random(cfg["seed"])
    indexes = rng.sample(range(cfg["players"]), min(samples, cfg["players"]))
    mismatches = []
    for index in indexes:
        pid = Int64(cfg["player_id_base"] + index)
        actual = list(db[cfg["daily"]].find({"player_id": pid, "gaming_date": {"$lte": as_of(cfg)}}))
        expected_rows = list(daily_rows(cfg, index))
        if sorted(actual, key=lambda r: r["_id"]) != sorted(expected_rows, key=lambda r: r["_id"]):
            mismatches.append(f"daily source differs: {pid}")
        doc = db[cfg["profiles"]].find_one({"player_id": pid})
        if doc is None:
            mismatches.append(f"missing profile: {pid}")
            continue
        if run_id:
            expected = reference(cfg, expected_rows)
            got = doc.get("adt", {})
            if not expected_rows:
                # The finite job intentionally updates only patrons with activity.
                continue
            if got.get("run_id") != run_id or got.get("as_of") != as_of(cfg):
                mismatches.append(f"missing/stale ADT: {pid}")
            for w in cfg["windows"]:
                value = got.get(f"d{w}")
                if value is None or value.to_decimal() != expected[f"d{w}"]:
                    mismatches.append(f"incorrect ADT d{w}: {pid}")
            if got.get("active_days") != expected["active_days"]:
                mismatches.append(f"incorrect active-day counts: {pid}")
    count = db[cfg["profiles"]].count_documents({})
    if count != cfg["players"]:
        mismatches.append(f"profile count: {count} != {cfg['players']}")
    print(json.dumps({"samples": len(indexes), "mismatches": mismatches}, indent=2))
    if mismatches:
        raise SystemExit(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["setup", "load", "validate", "inspect", "preview"])
    parser.add_argument("--config")
    parser.add_argument("--sharded", action="store_true")
    parser.add_argument("--identities", action="store_true")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--run-id", help="Also verify sampled ADT writeback for this finite run")
    args = parser.parse_args()
    cfg = load_config(args.config)
    if not 0 <= args.partition < args.partitions or args.batch_size <= 0 or args.samples <= 0:
        parser.error("Invalid partition, batch size, or sample count")
    if args.command == "preview":
        print(json_util.dumps({"profile": profile(cfg, 0), "daily_sample": list(daily_rows(cfg, 0))[:2],
                               "adt_pipeline": pipeline(cfg, ids=[cfg["player_id_base"]])}, indent=2))
        return
    client, db = connect(cfg, "data")
    try:
        if args.command == "setup":
            setup(client, db, cfg, args.sharded, args.identities)
        elif args.command == "load":
            load(db, cfg, args.partition, args.partitions, args.batch_size, args.identities)
        elif args.command == "validate":
            validate(db, cfg, args.samples, args.run_id)
        elif args.command == "inspect":
            for name in (cfg["profiles"], cfg["daily"]):
                print(json_util.dumps({"collection": name, "indexes": list(db[name].list_indexes()),
                                       "storage": list(db[name].aggregate([{"$collStats": {"storageStats": {}}}]))}, indent=2))
            pid = Int64(cfg["player_id_base"])
            print(json_util.dumps({"profile_explain": db.command("explain", {
                "find": cfg["profiles"], "filter": {"player_id": pid}}, verbosity="executionStats"),
                "adt_explain": db.command("explain", {"aggregate": cfg["daily"],
                    "pipeline": pipeline(cfg, ids=[pid]), "cursor": {}}, verbosity="executionStats")}, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
