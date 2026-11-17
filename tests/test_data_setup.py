import copy
import unittest
from unittest.mock import MagicMock

from benchmark.config import DEFAULTS
from benchmark.data import setup


class DataSetupTests(unittest.TestCase):
    def make_client(self, shard_names):
        client = MagicMock()

        def admin_command(command):
            if command == "hello":
                return {"msg": "isdbgrid"}
            if command == "listShards":
                return {"shards": [{"_id": name} for name in shard_names]}
            return {"ok": 1}

        client.admin.command.side_effect = admin_command
        return client

    def shard_commands(self, client):
        return [call.args[0] for call in client.admin.command.call_args_list
                if isinstance(call.args[0], dict) and "shardCollection" in call.args[0]]

    def test_two_shard_setup_precreates_hashed_chunks_for_each_collection(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg["shards"] = 2
        client = self.make_client(["perf_0", "perf_1"])
        db = MagicMock()
        db.list_collection_names.return_value = []

        setup(client, db, cfg, sharded=True, identities=True)

        commands = self.shard_commands(client)
        self.assertEqual(len(commands), 3)
        self.assertEqual(
            [(command["shardCollection"], command["key"], command["numInitialChunks"])
             for command in commands],
            [
                ("casino_bench.member_profile", {"player_id": "hashed"}, 4),
                ("casino_bench.player_daily_metrics", {"player_id": "hashed"}, 4),
                ("casino_bench.member_identity", {"lookup_key": "hashed"}, 4),
            ],
        )

    def test_one_shard_setup_does_not_set_initial_chunk_count(self):
        cfg = copy.deepcopy(DEFAULTS)
        client = self.make_client(["perf_0"])
        db = MagicMock()
        db.list_collection_names.return_value = []

        setup(client, db, cfg, sharded=True, identities=False)

        commands = self.shard_commands(client)
        self.assertEqual(len(commands), 2)
        self.assertTrue(all("numInitialChunks" not in command for command in commands))

    def test_two_shard_setup_fails_if_mongos_has_only_one_shard(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg["shards"] = 2
        client = self.make_client(["perf_0"])
        db = MagicMock()

        with self.assertRaisesRegex(ValueError, "config expects 2 shards.*mongos reports 1"):
            setup(client, db, cfg, sharded=True, identities=True)

        db.create_collection.assert_not_called()
        self.assertEqual(self.shard_commands(client), [])


if __name__ == "__main__":
    unittest.main()
