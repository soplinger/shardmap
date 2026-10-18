import json
from pathlib import Path

import pytest

import shardmap
from shardmap import (
    ConfigError,
    Node,
    balance,
    build_ring,
    cluster_info,
    lookup,
    main,
    movement,
    parse_cluster,
    sample_keys,
    sweep,
)

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "cluster.txt"
BROKEN = Path(__file__).resolve().parent.parent / "examples" / "broken.txt"

GOOD = """
vnodes 100
replicas 2
node alpha
node beta weight=2
node gamma vnodes=50
"""


def write(tmp_path, text, name="cluster.txt"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------- parsing

def test_parse_reads_directives_and_nodes():
    cluster, warnings = parse_cluster(GOOD)
    assert cluster.vnodes == 100
    assert cluster.replicas == 2
    assert [n.name for n in cluster.nodes] == ["alpha", "beta", "gamma"]
    assert cluster.nodes[1].weight == 2
    assert cluster.nodes[2].vnodes == 50
    assert warnings == []


def test_parse_collects_all_problems_with_line_numbers():
    text = "vnodes 0\nnode a\nnode a\nnode b weight=abc\nnope\n"
    with pytest.raises(ConfigError) as info:
        parse_cluster(text)
    problems = dict(info.value.problems)
    assert problems[1] == "vnodes must be between 1 and 10000"
    assert problems[3] == "duplicate node 'a' (first defined on line 2)"
    assert "bad option 'weight=abc'" in problems[4]
    assert problems[5] == "unknown directive 'nope'"


def test_parse_rejects_empty_cluster_and_bad_replicas():
    with pytest.raises(ConfigError) as info:
        parse_cluster("vnodes 10\n")
    assert info.value.problems == [(None, "no nodes defined")]

    with pytest.raises(ConfigError) as info:
        parse_cluster("replicas 3\nnode a\nnode b\n")
    assert info.value.problems[0][0] == 1
    assert "replicas must be between 1 and 2" in info.value.problems[0][1]


def test_parse_warns_on_few_ring_points():
    cluster, warnings = parse_cluster("vnodes 10\nnode tiny\nnode big vnodes=200\n")
    assert [line for line, _ in warnings] == [2]
    assert "only 10 ring points" in warnings[0][1]


def test_parse_strips_comments_and_ignores_blank_lines():
    cluster, _ = parse_cluster("# header\n\nnode a # trailing comment\n\n")
    assert [n.name for n in cluster.nodes] == ["a"]


def test_example_cluster_parses_cleanly():
    cluster, warnings = parse_cluster(EXAMPLE.read_text(encoding="utf-8"))
    assert len(cluster.nodes) == 3
    assert cluster.replicas == 2
    assert warnings == []


# ---------------------------------------------------------------- ring behaviour

def test_ring_is_deterministic_and_covers_every_node():
    nodes = [Node("a"), Node("b"), Node("c")]
    ring_one = build_ring(nodes, 50)
    ring_two = build_ring(nodes, 50)
    keys = sample_keys(500, 1)
    assert [ring_one.owner(k) for k in keys] == [ring_two.owner(k) for k in keys]
    assert set(ring_one.owner(k) for k in keys) == {"a", "b", "c"}


def test_preference_list_is_distinct_and_starts_with_owner():
    ring = build_ring([Node("a"), Node("b"), Node("c")], 30)
    for key in sample_keys(200, 2):
        prefs = ring.preference(key, 3)
        assert prefs[0] == ring.owner(key)
        assert len(prefs) == len(set(prefs)) == 3


def test_preference_list_never_exceeds_node_count():
    ring = build_ring([Node("a"), Node("b")], 20)
    assert len(ring.preference("some-key", 5)) == 2


def test_lookup_reports_owner_and_replicas():
    cluster, _ = parse_cluster(GOOD)
    result = lookup(cluster, ["user:1", "user:2"])
    assert [r["key"] for r in result] == ["user:1", "user:2"]
    for row in result:
        assert len(row["replicas"]) == 2
        assert row["replicas"][0] == row["owner"]


def test_adding_a_node_only_moves_keys_to_it():
    base = [Node("a"), Node("b"), Node("c")]
    before = build_ring(base, 80)
    after = build_ring(base + [Node("d")], 80)
    for key in sample_keys(3000, 3):
        if before.owner(key) != after.owner(key):
            assert after.owner(key) == "d"


# ---------------------------------------------------------------- analyses

def test_info_fair_shares_follow_weights():
    cluster, _ = parse_cluster(GOOD)
    data = cluster_info(cluster)
    shares = {r["node"]: r["fair_share_pct"] for r in data["rows"]}
    assert shares == {"alpha": 25.0, "beta": 50.0, "gamma": 25.0}
    assert data["total_points"] == 100 + 200 + 50


def test_balance_is_close_to_fair_with_many_vnodes():
    cluster, _ = parse_cluster("vnodes 400\nnode a\nnode b\nnode c\n")
    data = balance(cluster, 30000, 7)
    assert sum(r["keys"] for r in data["rows"]) == 30000
    assert data["max_ratio"] < 1.25


def test_balance_respects_weights():
    cluster, _ = parse_cluster("vnodes 400\nnode small\nnode large weight=3\n")
    data = balance(cluster, 20000, 9)
    rows = {r["node"]: r for r in data["rows"]}
    assert rows["large"]["keys"] > 2 * rows["small"]["keys"]


def test_balance_is_reproducible_for_same_seed():
    cluster, _ = parse_cluster(GOOD)
    assert balance(cluster, 2000, 4) == balance(cluster, 2000, 4)
    assert balance(cluster, 2000, 4) != balance(cluster, 2000, 5)


def test_movement_add_moves_about_the_fair_share_and_nothing_misplaced():
    cluster, _ = parse_cluster("vnodes 300\nnode a\nnode b\nnode c\n")
    data = movement(cluster, 20000, 11, add="d")
    assert data["misplaced"] == 0
    assert abs(data["excess"]) < 0.05
    assert abs(data["expected"] - 0.25) < 0.01


def test_movement_remove_only_moves_keys_from_removed_node():
    cluster, _ = parse_cluster("vnodes 300\nnode a\nnode b\nnode c\n")
    data = movement(cluster, 20000, 11, remove="b")
    assert data["action"] == "remove"
    assert data["misplaced"] == 0
    assert abs(data["expected"] - 1 / 3) < 0.01


def test_movement_refuses_to_remove_last_node():
    cluster, _ = parse_cluster("node only\n")
    with pytest.raises(shardmap.UsageError):
        movement(cluster, 100, 0, remove="only")


def test_sweep_more_vnodes_gives_better_balance_and_recommendation():
    cluster, _ = parse_cluster("node a\nnode b\nnode c\nnode d\n")
    data = sweep(cluster, [4, 64, 512], 20000, 5, target=1.25)
    ratios = [r["max_ratio"] for r in data["rows"]]
    assert ratios[0] > ratios[-1]
    assert data["recommended"] is not None
    assert data["recommended"] in (64, 512)
    assert [r["vnodes"] for r in data["rows"]] == [4, 64, 512]


def test_sweep_sorts_and_dedupes_values():
    cluster, _ = parse_cluster("node a\nnode b\n")
    data = sweep(cluster, [32, 8, 32], 500, 0)
    assert [r["vnodes"] for r in data["rows"]] == [8, 32]


def test_sample_keys_are_seeded():
    assert sample_keys(5, 1) == sample_keys(5, 1)
    assert sample_keys(5, 1) != sample_keys(5, 2)


# ---------------------------------------------------------------- CLI

def test_cli_lint_ok_on_example(capsys):
    assert main(["lint", str(EXAMPLE)]) == 0
    assert "ok: 3 nodes, 160 vnodes, 2 replica(s)" in capsys.readouterr().out


def test_cli_lint_reports_line_numbered_errors(capsys):
    assert main(["lint", str(BROKEN)]) == 1
    out = capsys.readouterr().out
    assert f"{BROKEN}:1: error: vnodes must be between 1 and 10000" in out
    assert f"{BROKEN}:2: error: replicas must be between 1 and 1" in out
    assert f"{BROKEN}:4: error: duplicate node 'cache-a'" in out
    assert f"{BROKEN}:5: error: bad option 'b'" in out
    assert f"{BROKEN}:6: error: bad option 'weight=abc'" in out
    assert f"{BROKEN}:7: error: unknown directive 'bogus'" in out


def test_cli_lint_strict_fails_on_warnings(tmp_path, capsys):
    path = write(tmp_path, "vnodes 5\nnode a\nnode b\n")
    assert main(["lint", path]) == 0
    assert main(["lint", "--strict", path]) == 1
    assert "warning" in capsys.readouterr().out


def test_cli_missing_file_is_usage_error(tmp_path, capsys):
    assert main(["info", str(tmp_path / "nope.txt")]) == 2
    assert "cannot read" in capsys.readouterr().err


def test_cli_info_json(capsys):
    assert main(["info", "--json", str(EXAMPLE)]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["nodes"] == 3
    assert data["total_points"] == 160 * 4


def test_cli_lookup_text_and_json(capsys):
    assert main(["lookup", str(EXAMPLE), "order-17"]) == 0
    line = capsys.readouterr().out.strip()
    assert line.startswith("order-17  ->  ")
    assert "replicas:" in line

    assert main(["lookup", "--json", str(EXAMPLE), "a", "b"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert [r["key"] for r in rows] == ["a", "b"]


def test_cli_balance_gate_passes_and_fails(capsys):
    assert main(["balance", "--keys", "5000", "--max-imbalance", "50", str(EXAMPLE)]) == 0
    capsys.readouterr()
    assert main(["balance", "--keys", "5000", "--max-imbalance", "0.5", str(EXAMPLE)]) == 3
    assert "GATE FAILED" in capsys.readouterr().out


def test_cli_movement_gate_and_usage_errors(tmp_path, capsys):
    path = write(tmp_path, "vnodes 200\nnode a\nnode b\n")
    assert main(["movement", "--add", "c", "--keys", "3000", "--max-excess", "0.5", path]) == 0
    capsys.readouterr()
    assert main(["movement", "--remove", "zzz", path]) == 2
    assert "not in the cluster" in capsys.readouterr().err
    assert main(["movement", "--add", "a", path]) == 2
    assert "already in the cluster" in capsys.readouterr().err
    with pytest.raises(SystemExit) as info:
        main(["movement", "--add", "c", "--keys", "3000", "--max-excess", "-1", path])
    assert info.value.code == 2


def test_cli_sweep_prints_recommendation(capsys):
    assert main(["sweep", "--values", "8,256", "--target", "3", "--keys", "2000", str(EXAMPLE)]) == 0
    out = capsys.readouterr().out
    assert "recommended: 8 vnodes per unit weight" in out


def test_cli_rejects_bad_numbers(capsys):
    with pytest.raises(SystemExit) as info:
        main(["balance", "--keys", "0", str(EXAMPLE)])
    assert info.value.code == 2
    capsys.readouterr()
    with pytest.raises(SystemExit):
        main(["sweep", "--values", "1,abc", str(EXAMPLE)])
