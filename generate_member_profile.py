#!/usr/bin/env python3
"""
Synthetic data generator for the SJM member_profile sharding benchmark.

Schema follows SJM member_profile_2025-04-10.xlsx (fields, types, bucket_bal groups).
All values are synthetic - no real customer data / PII.

What it does (through mongos):
  1. Creates <db> with primary shard = --primary-shard (default myShard_0)
  2. Creates <db>.member_profile_before   (unsharded -> stays on the primary shard)
  3. Creates <db>.member_profile_after    (sharded on { player_id: "hashed" } while empty)
  4. Generates N deterministic documents and inserts the SAME documents into both
  5. Builds the same secondary indexes on both (after loading, faster)
  6. Prints count / avg doc size / per-shard distribution

Examples:
  # preview 3 docs + average BSON size, no database needed
  python generate_member_profile.py --dry-run 3

  # full load: 3M members into both collections, 8 parallel loader processes
  python generate_member_profile.py --uri "mongodb://<user>:<pwd>@<mongos>:27017/?authSource=admin" \
      --db sjm_bench --count 3000000 --workers 8 --drop

  # pad documents to the real average size (ask the DBA for db.member_profile.stats().avgObjSize)
  python generate_member_profile.py ... --target-avg-bytes 4096
"""
import argparse
import json
import multiprocessing as mp
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone

from bson import Int64
from bson.decimal128 import Decimal128

# ---------------------------------------------------------------- reference data
REF_DATE = datetime(2026, 11, 1, tzinfo=timezone.utc)   # fixed -> reproducible data
PLAYER_ID_BASE = 10_000_000

# bucket_bal groups from the workbook's bucket_bal_calculation sheet
BUCKETS_BASE = [(1, "Comp_Base", 2, "Comp"), (2, "PvDollar_Base", 1, "Point"),
                (3, "Stubs_Base", 3, "Stub"), (4, "eBonus_Base", 4, "Promo1"),
                (5, "TierPt_Base", 5, "TierPoints")]
BUCKETS_EXTRA = [(10000010, "NonGamingBonus", 1, "Point"), (10000020, "GLPromotionalBucket", 1, "Point"),
                 (10000021, "GLCPromotionalBucket", 1, "Point"), (10000022, "GLPromotionStub", 3, "Stub")]

# synthetic lookup values (not SJM's real code tables)
CLUB_LEVELS = [(1, "Classic"), (2, "Silver"), (3, "Gold"), (4, "Platinum"), (5, "Diamond")]
CLUB_W = [55, 25, 12, 6, 2]
COUNTRIES = [(1, "MO"), (2, "HK"), (3, "CN"), (4, "TW"), (5, "MY"), (6, "SG"),
             (7, "PH"), (8, "JP"), (9, "KR"), (10, "TH")]
COUNTRY_W = [20, 25, 35, 4, 3, 3, 3, 3, 2, 2]
CONTACT_TYPES = ["Home", "Business", "Mobile", "Other"]
IDENT_CODES = ["PASSPORT", "HKID", "MOID", "CNID", "TWID"]
TAG_CODES = [f"TAG{n:03d}" for n in range(1, 201)]
LANGS = ["zh-HK", "zh-CN", "en-US", "pt-PT", "ja-JP", "ko-KR"]
CITIES = ["Macau", "Taipa", "Coloane", "Hong Kong", "Kowloon", "Zhuhai",
          "Guangzhou", "Shenzhen", "Taipei", "Singapore"]
FIRST = ["Wei", "Ming", "Hui", "Ka", "Chun", "Mei", "Jun", "Yan", "Ho", "Lok", "John", "Mary",
         "David", "Grace", "Kenji", "Min", "Ana", "Paulo", "Siti", "Arjun"]
LAST = ["Chan", "Wong", "Leung", "Lee", "Cheung", "Lau", "Ho", "Ng", "Chen", "Liu", "Zhang",
        "Wang", "Smith", "Tan", "Lim", "Sato", "Kim", "Silva", "Santos", "Lopez"]
TITLES = ["Mr.", "Ms.", "Mrs.", "Dr."]
HOSTS = list(range(5001, 5301))   # 300 synthetic casino hosts

DEFAULT_INDEXES = [   # representative 17 + _id = 18 indexes (SJM reported 18 on member_profile)
    {"player_id": 1}, {"member_no": 1}, {"member_card.card_num": 1},
    {"primary_host_num": 1}, {"secondary_host_num": 1}, {"club_state": 1},
    {"club_state_level_id": 1}, {"pinfo.first_name": 1}, {"pinfo.last_name": 1},
    {"pinfo.birth_date": 1}, {"pinfo_phone.phone_number": 1}, {"pinfo_email.email_address": 1},
    {"tag.tag_code": 1}, {"setup_datetime": 1}, {"setup_casino_id": 1},
    {"is_vip": 1}, {"_ODS_modified_dtm": 1},
]

_PAD_BLOB = "".join(random.Random(20261113).choices(string.ascii_letters + string.digits, k=1_048_576))


# ---------------------------------------------------------------- document builder
def member_no_of(i):
    # bijective scramble of i -> unique, non-sequential 10-digit card number
    return f"8{(i * 7919 + 1_000_003) % 1_000_000_000:09d}"


def _date(rng, start_days_ago, end_days_ago=0):
    return REF_DATE - timedelta(days=rng.uniform(end_days_ago, start_days_ago))


def _dec(rng, hi):
    return Decimal128(f"{rng.uniform(0, hi):.2f}")


def gen_doc(rng, i, pad_bytes=0):
    pid = PLAYER_ID_BASE + i
    mno = member_no_of(i)
    first, last = rng.choice(FIRST), rng.choice(LAST)
    gender = rng.randint(0, 1)
    birth = _date(rng, 365 * 80, 365 * 18)
    cid, ccode = rng.choices(COUNTRIES, COUNTRY_W)[0]
    lvl_id, lvl_name = rng.choices(CLUB_LEVELS, CLUB_W)[0]
    setup = _date(rng, 365 * 15, 1)
    modified = _date(rng, (REF_DATE - setup).days or 1)
    p_host = rng.choice(HOSTS) if rng.random() < 0.35 else None
    s_host = rng.choice(HOSTS) if p_host and rng.random() < 0.5 else None

    cards = []
    if rng.random() < 0.95:
        cards.append({"swipe_data": f";{mno}=0{rng.randint(100, 999)}?", "card_num": mno, "is_inactive": False})
        if rng.random() < 0.15:
            alt = f"9{(i * 104729 + 7) % 1_000_000_000:09d}"
            cards.append({"swipe_data": f";{alt}=0{rng.randint(100, 999)}?", "card_num": alt, "is_inactive": False})

    buckets = BUCKETS_BASE + rng.sample(BUCKETS_EXTRA, rng.randint(0, len(BUCKETS_EXTRA)))
    bucket_bal = [{"group_id": g, "group_name": gn, "type_id": t, "type_name": tn,
                   "amount": _dec(rng, 50_000 if t != 5 else 500_000)} for g, gn, t, tn in buckets]

    phones = [{"phone_number": f"+853 6{rng.randint(0, 9_999_999):07d}", "contact_type": rng.choice(CONTACT_TYPES)}
              for _ in range(rng.choices([0, 1, 2, 3], [5, 60, 25, 10])[0])]
    emails = [{"email_address": f"{first}.{last}.{i}{'' if n == 0 else n}@example.com".lower(),
               "contact_type": rng.choice(CONTACT_TYPES)}
              for n in range(rng.choices([0, 1, 2], [20, 70, 10])[0])]
    addresses = [{"address_1": f"{rng.randint(1, 999)} {rng.choice(['Avenida', 'Rua', 'Road', 'Street'])} {rng.choice(LAST)}",
                  "address_2": f"Flat {rng.randint(1, 40)}{rng.choice('ABCDEFGH')}", "city": rng.choice(CITIES),
                  "state_code": "", "country_code": ccode, "contact_type": rng.choice(CONTACT_TYPES)}
                 for _ in range(rng.choices([0, 1, 2], [15, 70, 15])[0])]
    identities = [{"id_index": n + 1, "player_identity": f"{rng.choice('ABCDEHKMPRZ')}{rng.randint(1_000_000, 9_999_999)}",
                   "issue_dt": _date(rng, 365 * 10, 365), "expire_dt": REF_DATE + timedelta(days=rng.randint(30, 3650)),
                   "ident_code": rng.choice(IDENT_CODES)}
                  for n in range(rng.choices([0, 1, 2], [10, 75, 15])[0])]
    attribute = [{"birth_dt": birth, "wedding_date": _date(rng, 365 * 40, 365) if rng.random() < 0.3 else None,
                  "smoker": rng.random() < 0.2, "sex": bool(gender), "marital": rng.randint(0, 3),
                  "height": "", "weight": "", "eyes": "", "hair": "",
                  "citizenship_country_id": cid, "beverage": rng.choice(["", "Tea", "Coffee", "Wine", "Whisky"]),
                  "occupation": rng.choice(["", "Business", "Professional", "Retired", "Other"]),
                  "ethnicity": "", "age_group": f"{(REF_DATE.year - birth.year) // 10 * 10}s",
                  "hair_type": "", "hair_color": "", "attire": "", "attire_color": "",
                  "language_code": rng.choice(LANGS)}]
    tags = [{"tag_code": t} for t in rng.sample(TAG_CODES, rng.choices([0, 1, 2, 3, 5], [40, 25, 15, 12, 8])[0])]

    doc = {
        "_ODS_schema_version": 2,
        "player_id": Int64(pid),
        "member_no": mno,
        "is_card_holder": bool(cards),
        "member_card": cards,
        "club_state": rng.choices([0, 1, 2], [80, 15, 5])[0],
        "club_state_level_id": lvl_id,
        "club_state_name": lvl_name,
        "club_state_expiry_date": REF_DATE + timedelta(days=rng.randint(0, 730)),
        "club_state_qualifying_date": _date(rng, 730),
        "primary_host_id": p_host,
        "primary_host_num": f"E{p_host}" if p_host else None,
        "secondary_host_id": s_host,
        "secondary_host_num": f"E{s_host}" if s_host else None,
        "is_active_program": rng.random() < 0.05,
        "is_active_pptrip": rng.random() < 0.03,
        "setup_datetime": setup,
        "setup_casino_id": rng.randint(1, 20),
        "is_banned": rng.random() < 0.01,
        "is_inactive": rng.random() < 0.10,
        "is_retired": rng.random() < 0.02,
        "is_vip": lvl_id >= 4,
        "is_whale": rng.random() < 0.002,
        "is_deactivate": rng.random() < 0.01,
        "is_hide": False,
        "bucket_bal": bucket_bal,
        "pinfo": {"first_name": first, "last_name": last, "birth_date": birth,
                  "nick_name": first if rng.random() < 0.3 else "", "title": rng.choice(TITLES),
                  "gender": gender, "country_id": cid, "country_code": ccode},
        "pinfo_address": addresses,
        "pinfo_identity": identities,
        "pinfo_attribute": attribute,
        "pinfo_phone": phones,
        "pinfo_email": emails,
        "tag": tags,
        "_ODS_created_dtm": setup,
        "_ODS_modified_dtm": modified,
        "_ODS_is_deleted": False,
        "_ODS_replay_switch": False,
    }
    if pad_bytes > 0:
        off = rng.randint(0, len(_PAD_BLOB) - pad_bytes - 1)
        doc["_bench_pad"] = _PAD_BLOB[off:off + pad_bytes]   # random text -> not over-compressible
    return doc


def gen_batch(seed, batch_no, batch_size, count, pad_bytes):
    rng = random.Random(seed * 1_000_003 + batch_no)        # deterministic per batch
    start = batch_no * batch_size
    return [gen_doc(rng, i, pad_bytes) for i in range(start, min(start + batch_size, count))]


# ---------------------------------------------------------------- loader processes
_CTX = {}


def _init_worker(uri, db, colls, wc):
    from pymongo import MongoClient
    client = MongoClient(uri, w=wc, appname="member-profile-generator")
    _CTX["colls"] = [client[db][c] for c in colls]


def _load_batches(task):
    seed, batch_nos, batch_size, count, pad_bytes = task
    n = 0
    for b in batch_nos:
        docs = gen_batch(seed, b, batch_size, count, pad_bytes)
        for coll in _CTX["colls"]:
            coll.insert_many([dict(d) for d in docs], ordered=False)
        n += len(docs)
    return n


# ---------------------------------------------------------------- setup / indexes / report
def setup(client, args):
    from pymongo.errors import OperationFailure
    db = client[args.db]
    is_mongos = client.admin.command("hello").get("msg") == "isdbgrid"
    if args.drop:
        for c in (args.before_coll, args.after_coll):
            db.drop_collection(c)
            print(f"dropped {args.db}.{c}")
    if is_mongos:
        try:
            client.admin.command({"enableSharding": args.db, "primaryShard": args.primary_shard})
        except OperationFailure as e:
            print(f"enableSharding: {e}")
        primary = (client.config.databases.find_one({"_id": args.db}) or {}).get("primary")
        print(f"database {args.db} primary shard = {primary}")
        if primary and primary != args.primary_shard:
            print(f"WARNING: primary is {primary}, not {args.primary_shard}. "
                  f"Run: db.adminCommand({{movePrimary: '{args.db}', to: '{args.primary_shard}'}})")
    else:
        print("WARNING: not connected to mongos - sharding steps skipped (only 'before' is meaningful)")

    existing = db.list_collection_names()
    if args.before_coll not in existing:
        db.create_collection(args.before_coll)
    if args.after_coll not in existing:
        db.create_collection(args.after_coll)
    if is_mongos:
        db[args.after_coll].create_index([("player_id", "hashed")])
        try:
            client.admin.command({"shardCollection": f"{args.db}.{args.after_coll}",
                                  "key": {"player_id": "hashed"}})
            print(f"sharded {args.db}.{args.after_coll} on {{player_id: 'hashed'}}")
        except OperationFailure as e:
            print(f"shardCollection: {e}")
    for c in (args.before_coll, args.after_coll):
        if db[c].estimated_document_count() > 0:
            sys.exit(f"{args.db}.{c} is not empty - use --drop to reload")


def build_indexes(client, args):
    specs = DEFAULT_INDEXES
    if args.indexes_json:
        # format: output of db.member_profile.getIndexes() saved as JSON (only "key" is used)
        specs = [ix["key"] for ix in json.load(open(args.indexes_json)) if ix.get("name") != "_id_"]
    for c in (args.before_coll, args.after_coll):
        t = time.time()
        for key in specs:
            client[args.db][c].create_index(list(key.items()))   # non-unique on both, for fairness
        print(f"built {len(specs)} indexes on {args.db}.{c} in {time.time() - t:.0f}s")


def report(client, args):
    for c in (args.before_coll, args.after_coll):
        print(f"\n{args.db}.{c}")
        for s in client[args.db][c].aggregate([{"$collStats": {"storageStats": {}}}]):
            st = s["storageStats"]
            print(f"  shard={s.get('shard', '-'):<12} docs={st['count']:>10,} "
                  f"avgObjSize={st.get('avgObjSize', 0):>6,.0f}B dataSize={st['size'] / 2**30:6.2f}GB "
                  f"storage={st['storageSize'] / 2**30:6.2f}GB indexes={st['totalIndexSize'] / 2**30:6.2f}GB")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="sjm_bench")
    ap.add_argument("--before-coll", default="member_profile_before")
    ap.add_argument("--after-coll", default="member_profile_after")
    ap.add_argument("--primary-shard", default="myShard_0")
    ap.add_argument("--count", type=int, default=3_000_000)
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=max(1, mp.cpu_count() - 1))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--target-avg-bytes", type=int, default=0, help="pad docs to this average BSON size")
    ap.add_argument("--write-concern", default="1", help="'1' for fast loading, or 'majority'")
    ap.add_argument("--indexes-json", help="JSON from getIndexes() of the real collection (optional)")
    ap.add_argument("--drop", action="store_true", help="drop both collections first")
    ap.add_argument("--skip-indexes", action="store_true")
    ap.add_argument("--dry-run", type=int, metavar="N", help="print N sample docs and size stats, no DB")
    args = ap.parse_args()

    pad = 0
    if args.target_avg_bytes or args.dry_run:
        import bson
        sample = gen_batch(args.seed, 0, 1000, 1000, 0)
        avg = sum(len(bson.encode(d)) for d in sample) / len(sample)
        pad = max(0, args.target_avg_bytes - int(avg) - 20) if args.target_avg_bytes else 0
        print(f"natural avg BSON size = {avg:,.0f} bytes; padding per doc = {pad:,} bytes")
    if args.dry_run:
        for d in gen_batch(args.seed, 0, args.dry_run, args.dry_run, pad):
            print(json.dumps(d, default=str, indent=2))
        return

    from pymongo import MongoClient
    wc = int(args.write_concern) if args.write_concern.isdigit() else args.write_concern
    client = MongoClient(args.uri)
    setup(client, args)

    n_batches = (args.count + args.batch_size - 1) // args.batch_size
    chunk = 10
    tasks = [(args.seed, list(range(b, min(b + chunk, n_batches))), args.batch_size, args.count, pad)
             for b in range(0, n_batches, chunk)]
    colls = [args.before_coll, args.after_coll]
    print(f"loading {args.count:,} docs into {colls} with {args.workers} workers ...")
    t0, done = time.time(), 0
    with mp.Pool(args.workers, initializer=_init_worker, initargs=(args.uri, args.db, colls, wc)) as pool:
        for n in pool.imap_unordered(_load_batches, tasks):
            done += n
            if done % 100_000 < n or done == args.count:
                rate = done / (time.time() - t0)
                print(f"  {done:>10,}/{args.count:,}  {rate:,.0f} docs/s  "
                      f"eta {(args.count - done) / rate / 60:5.1f} min", flush=True)
    print(f"load finished in {(time.time() - t0) / 60:.1f} min")

    if not args.skip_indexes:
        build_indexes(client, args)
    report(client, args)


if __name__ == "__main__":
    main()
