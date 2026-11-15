"""Validated configuration shared by the loader, workers, and reporting tools."""
import copy
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml


DEFAULTS = {
    "label": "local-s1",
    "database": "casino_bench",
    "profiles": "member_profile",
    "daily": "player_daily_metrics",
    "identities": "member_identity",
    "shards": 1,
    "scenario": "profile",
    "seed": 42,
    "as_of": "2026-11-01",
    "players": 10000,
    "player_id_base": 10000000,
    "history_days": 180,
    "active_day_probability": 0.2,
    "windows": [7, 30, 90, 180],
    "denominator": "active",
    "distribution": "uniform",
    "hot_fraction": 0.2,
    "hot_probability": 0.8,
    "warmup_seconds": 60,
    "mongo": {"max_pool_size": 100, "timeout_ms": 60000, "write_concern": "majority",
              "read_concern": "local", "read_preference": "primary"},
    "profile_weights": {"read_full": 65, "update_contact": 15, "read_update": 10,
                        "member_lookup": 5, "host_list": 5},
    "batch": {"size": 200, "slots_per_worker": 1, "lease_seconds": 300,
              "max_attempts": 3, "mode": "ids", "writeback": True},
    "load": {"mode": "concurrency", "rate": 100, "max_inflight_per_worker": 100},
}


def _merge(base, override):
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge(base[key], value)
        else:
            base[key] = value
    return base


def load_config(path=None):
    cfg = copy.deepcopy(DEFAULTS)
    path = path or os.getenv("BENCH_CONFIG", "config/smoke.yaml")
    with open(path, encoding="utf-8") as handle:
        overrides = yaml.safe_load(handle) or {}
    _merge(cfg, overrides)
    for env, key in (("BENCH_LABEL", "label"), ("BENCH_SCENARIO", "scenario")):
        if os.getenv(env):
            cfg[key] = os.environ[env]
    if cfg["scenario"] not in {"profile", "adt", "incremental", "ingest"}:
        raise ValueError("Unknown scenario")
    if not cfg["windows"] or any(w <= 0 for w in cfg["windows"]):
        raise ValueError("ADT windows must be positive")
    if cfg["players"] <= 0 or cfg["history_days"] < max(cfg["windows"]):
        raise ValueError("players must be positive; history_days must cover all ADT windows")
    if cfg["denominator"] not in {"active", "window"}:
        raise ValueError("denominator must be active or window")
    if cfg["distribution"] not in {"uniform", "skewed", "hotspot"}:
        raise ValueError("Unknown distribution")
    if not 0 < cfg["hot_fraction"] <= 1 or not 0 <= cfg["hot_probability"] <= 1:
        raise ValueError("Invalid hot population settings")
    if not 0 <= cfg["active_day_probability"] <= 1 or cfg["warmup_seconds"] < 0:
        raise ValueError("Invalid activity probability or warmup")
    if cfg["batch"]["mode"] not in {"ids", "range"}:
        raise ValueError("batch.mode must be ids or range")
    if any(cfg["batch"][k] <= 0 for k in ("size", "slots_per_worker", "lease_seconds", "max_attempts")):
        raise ValueError("Batch sizes, slots, lease, and attempts must be positive")
    weights = cfg["profile_weights"]
    allowed = {"read_full", "read_projected", "update_contact", "update_bucket", "read_update",
               "member_lookup", "card_lookup", "host_list", "identity_journey"}
    if set(weights) - allowed or any(v < 0 for v in weights.values()) or sum(weights.values()) <= 0:
        raise ValueError("Invalid profile_weights")
    if cfg["load"]["mode"] not in {"concurrency", "arrival"}:
        raise ValueError("load.mode must be concurrency or arrival")
    if cfg["load"]["rate"] <= 0 or cfg["load"]["max_inflight_per_worker"] <= 0:
        raise ValueError("Arrival rate and inflight limit must be positive")
    cfg["windows"] = sorted(set(cfg["windows"]))
    as_of(cfg)  # validate date early
    return cfg


def as_of(cfg):
    return datetime.strptime(cfg["as_of"], "%Y-%m-%d").replace(tzinfo=timezone.utc)


def config_hash(cfg):
    return hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()


def output_dir():
    path = Path(os.getenv("BENCH_RESULTS", "result"))
    path.mkdir(parents=True, exist_ok=True)
    return path
