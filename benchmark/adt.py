"""Server aggregation and independent Decimal reference implementation."""
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_EVEN

from bson import Int64

from benchmark.config import as_of


def pipeline(cfg, ids=None, bounds=None):
    end = as_of(cfg) + timedelta(days=1)
    starts = {w: end - timedelta(days=w) for w in cfg["windows"]}
    player_match = {"$in": [Int64(p) for p in ids]} if ids is not None else {
        "$gte": Int64(bounds[0]), "$lt": Int64(bounds[1])}
    group = {"_id": "$player_id"}
    projection = {"_id": 1, "adt": {"active_days": {}}}
    for w in cfg["windows"]:
        condition = {"$gte": ["$gaming_date", starts[w]]}
        group[f"t{w}"] = {"$sum": {"$cond": [condition, "$gaming.total.theo_win", 0]}}
        group[f"a{w}"] = {"$sum": {"$cond": [condition, 1, 0]}}
        divisor = w if cfg["denominator"] == "window" else {"$max": [f"$a{w}", 1]}
        projection["adt"][f"d{w}"] = {"$round": [{"$divide": [f"$t{w}", divisor]}, 2]}
        projection["adt"]["active_days"][f"d{w}"] = f"$a{w}"
    return [{"$match": {"player_id": player_match,
                         "gaming_date": {"$gte": starts[max(starts)], "$lt": end}}},
            {"$group": group}, {"$project": projection}]


def reference(cfg, rows):
    end = as_of(cfg) + timedelta(days=1)
    result = {"active_days": {}}
    for w in cfg["windows"]:
        values = [r["gaming"]["total"]["theo_win"].to_decimal() for r in rows
                  if end - timedelta(days=w) <= r["gaming_date"] < end]
        denominator = w if cfg["denominator"] == "window" else max(len(values), 1)
        result[f"d{w}"] = (sum(values, Decimal(0)) / denominator).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN)
        result["active_days"][f"d{w}"] = len(values)
    return result
