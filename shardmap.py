#!/usr/bin/env python3
"""Shardmap: an offline consistent-hash ring planner.

Reads a plain-text cluster file, places weighted virtual nodes on a hash ring,
and reports key lookups, replica sets, load balance, key movement when a node
joins or leaves, and a virtual-node sweep. Standard library only; no network.
"""
from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import random
import re
import sys
from dataclasses import dataclass, field

EXIT_OK = 0
EXIT_ERRORS = 1
EXIT_USAGE = 2
EXIT_GATE = 3

DEFAULT_VNODES = 100
MAX_VNODES = 10_000
MAX_WEIGHT = 100
MAX_POINTS = 1_000_000
MIN_SAFE_POINTS = 40
DEFAULT_KEYS = 100_000
DEFAULT_SWEEP = "16,32,64,128,256,512"

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class ConfigError(Exception):
    """Raised when a cluster file has problems. problems = [(lineno or None, message)]."""

    def __init__(self, problems):
        super().__init__(f"{len(problems)} problem(s)")
        self.problems = problems


class UsageError(Exception):
    """Raised for bad command-line requests that are valid files but impossible asks."""


@dataclass
class Node:
    name: str
    weight: int = 1
    vnodes: int | None = None  # per-node override of the cluster default
    line: int = 0


@dataclass
class Cluster:
    nodes: list[Node] = field(default_factory=list)
    vnodes: int = DEFAULT_VNODES
    replicas: int = 1


# --------------------------------------------------------------------------- parsing

def _whole(text: str) -> int | None:
    """Return text as a non-negative int, or None if it is not plain digits."""
    return int(text) if re.fullmatch(r"[0-9]+", text) else None


def _parse_node(lineno, args, cluster, seen, problems):
    if not args:
        problems.append((lineno, "'node' needs a name"))
        return
    name, options = args[0], args[1:]
    if not NAME_RE.match(name):
        problems.append((lineno, f"invalid node name '{name}' (use letters, digits, '.', '_', '-')"))
        return
    if name in seen:
        problems.append((lineno, f"duplicate node '{name}' (first defined on line {seen[name]})"))
        return
    weight, vnodes, ok = 1, None, True
    for opt in options:
        key, sep, value = opt.partition("=")
        number = _whole(value) if sep else None
        if key == "weight" and number is not None and 1 <= number <= MAX_WEIGHT:
            weight = number
        elif key == "vnodes" and number is not None and 1 <= number <= MAX_VNODES:
            vnodes = number
        else:
            problems.append((lineno, f"bad option '{opt}' (use weight=N with N in 1..{MAX_WEIGHT}, or vnodes=N with N in 1..{MAX_VNODES})"))
            ok = False
    if not ok:
        return
    seen[name] = lineno
    cluster.nodes.append(Node(name, weight, vnodes, lineno))


def parse_cluster(text: str) -> tuple[Cluster, list[tuple[int, str]]]:
    """Parse cluster text. Returns (cluster, warnings) or raises ConfigError."""
    cluster = Cluster()
    problems: list[tuple[int | None, str]] = []
    seen: dict[str, int] = {}
    replicas_line = None
    for lineno, raw in enumerate(text.splitlines(), start=1):
        parts = raw.split("#", 1)[0].split()
        if not parts:
            continue
        head, args = parts[0].lower(), parts[1:]
        if head == "node":
            _parse_node(lineno, args, cluster, seen, problems)
        elif head in ("vnodes", "replicas"):
            if len(args) != 1:
                problems.append((lineno, f"'{head}' takes exactly one number"))
                continue
            value = _whole(args[0])
            if value is None:
                problems.append((lineno, f"'{args[0]}' is not a whole number"))
            elif head == "vnodes":
                if 1 <= value <= MAX_VNODES:
                    cluster.vnodes = value
                else:
                    problems.append((lineno, f"vnodes must be between 1 and {MAX_VNODES}"))
            else:
                cluster.replicas = value
                replicas_line = lineno
        else:
            problems.append((lineno, f"unknown directive '{parts[0]}'"))

    if not cluster.nodes:
        problems.append((None, "no nodes defined"))
    else:
        if not 1 <= cluster.replicas <= len(cluster.nodes):
            problems.append((replicas_line, f"replicas must be between 1 and {len(cluster.nodes)} (the node count)"))
        total = sum(node_points(n, cluster.vnodes) for n in cluster.nodes)
        if total > MAX_POINTS:
            problems.append((None, f"ring would hold {total} points (limit {MAX_POINTS}); lower vnodes or weights"))

    if problems:
        problems.sort(key=lambda p: (p[0] is None, p[0] or 0))
        raise ConfigError(problems)

    warnings = [
        (n.line, f"node '{n.name}' has only {node_points(n, cluster.vnodes)} ring points; "
                 f"expect uneven load below {MIN_SAFE_POINTS}")
        for n in cluster.nodes
        if node_points(n, cluster.vnodes) < MIN_SAFE_POINTS
    ]
    return cluster, warnings


# --------------------------------------------------------------------------- ring

def _hash(text: str) -> int:
    return int.from_bytes(hashlib.md5(text.encode("utf-8")).digest()[:8], "big")


def node_points(node: Node, default_vnodes: int) -> int:
    """Ring points for a node: its vnodes (or the default) times its weight."""
    return (node.vnodes or default_vnodes) * node.weight


class Ring:
    def __init__(self, layout: list[tuple[str, int]]):
        points = []
        for name, count in layout:
            for i in range(count):
                points.append((_hash(f"{name}#{i}"), name))
        points.sort()
        self.positions = [pos for pos, _ in points]
        self.owners = [name for _, name in points]
        self.names = [name for name, _ in layout]

    def owner(self, key: str) -> str:
        start = bisect.bisect_left(self.positions, _hash(key))
        return self.owners[start % len(self.owners)]

    def preference(self, key: str, count: int) -> list[str]:
        """Walk clockwise from the key collecting distinct nodes."""
        start = bisect.bisect_left(self.positions, _hash(key))
        picked: list[str] = []
        for step in range(len(self.owners)):
            name = self.owners[(start + step) % len(self.owners)]
            if name not in picked:
                picked.append(name)
                if len(picked) == count:
                    break
        return picked


def build_ring(nodes: list[Node], default_vnodes: int) -> Ring:
    return Ring([(n.name, node_points(n, default_vnodes)) for n in nodes])


def sample_keys(count: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    return [f"{rng.getrandbits(64):016x}" for _ in range(count)]


def count_loads(ring: Ring, keys: list[str]) -> dict[str, int]:
    loads = {name: 0 for name in ring.names}
    for key in keys:
        loads[ring.owner(key)] += 1
    return loads


def fair_shares(nodes: list[Node], keys_total: int) -> dict[str, float]:
    total_weight = sum(n.weight for n in nodes)
    return {n.name: keys_total * n.weight / total_weight for n in nodes}


# --------------------------------------------------------------------------- analyses

def cluster_info(cluster: Cluster) -> dict:
    total_points = sum(node_points(n, cluster.vnodes) for n in cluster.nodes)
    total_weight = sum(n.weight for n in cluster.nodes)
    rows = [
        {
            "node": n.name,
            "weight": n.weight,
            "points": node_points(n, cluster.vnodes),
            "points_pct": round(100 * node_points(n, cluster.vnodes) / total_points, 2),
            "fair_share_pct": round(100 * n.weight / total_weight, 2),
        }
        for n in cluster.nodes
    ]
    return {"nodes": len(cluster.nodes), "vnodes": cluster.vnodes,
            "replicas": cluster.replicas, "total_points": total_points, "rows": rows}


def balance(cluster: Cluster, keys_count: int, seed: int) -> dict:
    keys = sample_keys(keys_count, seed)
    loads = count_loads(build_ring(cluster.nodes, cluster.vnodes), keys)
    expected = fair_shares(cluster.nodes, keys_count)
    rows = []
    for n in cluster.nodes:
        fair = expected[n.name]
        rows.append({
            "node": n.name,
            "weight": n.weight,
            "keys": loads[n.name],
            "expected": round(fair, 1),
            "ratio": round(loads[n.name] / fair, 4),
            "deviation_pct": round(100 * (loads[n.name] - fair) / fair, 2),
        })
    worst = max(rows, key=lambda r: r["ratio"])
    return {"keys": keys_count, "seed": seed, "rows": rows,
            "max_ratio": worst["ratio"], "worst": worst["node"]}


def movement(cluster: Cluster, keys_count: int, seed: int,
             add: str | None = None, weight: int = 1, remove: str | None = None) -> dict:
    if add is not None:
        after_nodes = cluster.nodes + [Node(add, weight, None)]
        changed = Node(add, weight, None)
    else:
        after_nodes = [n for n in cluster.nodes if n.name != remove]
        changed = next(n for n in cluster.nodes if n.name == remove)
        if not after_nodes:
            raise UsageError("cannot remove the last node in the cluster")

    before = build_ring(cluster.nodes, cluster.vnodes)
    after = build_ring(after_nodes, cluster.vnodes)
    keys = sample_keys(keys_count, seed)

    moved = misplaced = 0
    for key in keys:
        old_owner, new_owner = before.owner(key), after.owner(key)
        if old_owner != new_owner:
            moved += 1
            # Consistent hashing: keys may only flow to a new node or away from a removed one.
            if (add is not None and new_owner != add) or (remove is not None and old_owner != remove):
                misplaced += 1

    points_before = sum(node_points(n, cluster.vnodes) for n in cluster.nodes)
    points_after = sum(node_points(n, cluster.vnodes) for n in after_nodes)
    if add is not None:
        expected = node_points(changed, cluster.vnodes) / points_after
    else:
        expected = node_points(changed, cluster.vnodes) / points_before
    observed = moved / keys_count
    return {
        "action": "add" if add is not None else "remove",
        "node": changed.name,
        "keys": keys_count,
        "moved": moved,
        "observed": round(observed, 4),
        "expected": round(expected, 4),
        "excess": round(observed - expected, 4),
        "misplaced": misplaced,
    }


def sweep(cluster: Cluster, values: list[int], keys_count: int, seed: int,
          target: float | None = None) -> dict:
    keys = sample_keys(keys_count, seed)
    total_weight = sum(n.weight for n in cluster.nodes)
    flat = [Node(n.name, n.weight, None, n.line) for n in cluster.nodes]
    expected = fair_shares(cluster.nodes, keys_count)
    rows = []
    for value in sorted(set(values)):
        loads = count_loads(build_ring(flat, value), keys)
        worst = max(cluster.nodes, key=lambda n: loads[n.name] / expected[n.name])
        ratio = loads[worst.name] / expected[worst.name]
        rows.append({
            "vnodes": value,
            "points": value * total_weight,
            "max_ratio": round(ratio, 4),
            "worst": worst.name,
            "meets_target": target is not None and ratio <= target,
        })
    recommended = next((r["vnodes"] for r in rows if r["meets_target"]), None)
    return {"keys": keys_count, "seed": seed, "target": target,
            "rows": rows, "recommended": recommended}


def lookup(cluster: Cluster, keys: list[str]) -> list[dict]:
    ring = build_ring(cluster.nodes, cluster.vnodes)
    return [{"key": k, "owner": ring.owner(k),
             "replicas": ring.preference(k, cluster.replicas)} for k in keys]


# --------------------------------------------------------------------------- output

def _table(headers: list[str], rows: list[list]) -> str:
    cells = [[str(c) for c in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h)
              for i, h in enumerate(headers)]
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)),
             "  ".join("-" * w for w in widths)]
    lines += ["  ".join(c.ljust(w) for c, w in zip(row, widths)) for row in cells]
    return "\n".join(lines)


def _dump(data) -> None:
    print(json.dumps(data, indent=2))


def _positive(text: str) -> int:
    value = _whole(text)
    if value is None or value < 1:
        raise argparse.ArgumentTypeError(f"'{text}' must be a whole number of at least 1")
    return value


def _ratio(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{text}' is not a number")
    if value < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return value


def _values(text: str) -> list[int]:
    values = []
    for piece in text.split(","):
        value = _whole(piece.strip())
        if value is None or not 1 <= value <= MAX_VNODES:
            raise argparse.ArgumentTypeError(f"'{piece}' is not a vnode count in 1..{MAX_VNODES}")
        values.append(value)
    return values


# --------------------------------------------------------------------------- CLI

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shardmap",
        description="Offline consistent-hash ring planner driven by a plain-text cluster file.")
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        sp = sub.add_parser(name, help=help_text, description=help_text)
        sp.add_argument("file", help="cluster file")
        return sp

    def add_sampling(sp):
        sp.add_argument("--keys", type=_positive, default=DEFAULT_KEYS,
                        help=f"synthetic keys to sample (default {DEFAULT_KEYS})")
        sp.add_argument("--seed", type=int, default=0, help="seed for the synthetic keys (default 0)")

    sp = add("info", "show ring points and fair shares for each node")
    sp.add_argument("--json", action="store_true", help="print JSON")

    sp = add("lint", "check the cluster file with line-numbered errors")
    sp.add_argument("--strict", action="store_true", help="treat warnings as errors")

    sp = add("lookup", "show the owning node and replica set for keys")
    sp.add_argument("key", nargs="+", help="one or more keys to place")
    sp.add_argument("--json", action="store_true", help="print JSON")

    sp = add("balance", "measure how evenly sampled keys land on each node")
    add_sampling(sp)
    sp.add_argument("--max-imbalance", type=_ratio, metavar="RATIO",
                    help="exit 3 if the busiest node exceeds RATIO times its fair share")
    sp.add_argument("--json", action="store_true", help="print JSON")

    sp = add("movement", "measure keys that move when a node joins or leaves")
    group = sp.add_mutually_exclusive_group(required=True)
    group.add_argument("--add", metavar="NAME", help="simulate adding a node")
    group.add_argument("--remove", metavar="NAME", help="simulate removing a node")
    sp.add_argument("--weight", type=_positive, default=1, help="weight for an added node (default 1)")
    add_sampling(sp)
    sp.add_argument("--max-excess", type=_ratio, metavar="FRACTION",
                    help="exit 3 if moved keys exceed the ideal share by more than FRACTION")
    sp.add_argument("--json", action="store_true", help="print JSON")

    sp = add("sweep", "compare balance across virtual-node counts")
    sp.add_argument("--values", type=_values, default=_values(DEFAULT_SWEEP),
                    help=f"comma-separated vnode counts (default {DEFAULT_SWEEP})")
    sp.add_argument("--target", type=_ratio, metavar="RATIO",
                    help="recommend the smallest vnode count whose busiest node is within RATIO")
    add_sampling(sp)
    sp.add_argument("--json", action="store_true", help="print JSON")
    return parser


def _cmd_info(cluster, args) -> int:
    data = cluster_info(cluster)
    if args.json:
        _dump(data)
        return EXIT_OK
    print(f"{data['nodes']} nodes, {data['vnodes']} vnodes per unit weight, "
          f"{data['replicas']} replica(s), {data['total_points']} ring points")
    print(_table(["node", "weight", "points", "points %", "fair share %"],
                 [[r["node"], r["weight"], r["points"], f"{r['points_pct']:.2f}",
                   f"{r['fair_share_pct']:.2f}"] for r in data["rows"]]))
    return EXIT_OK


def _cmd_lint(cluster, warnings, args, path) -> int:
    for lineno, message in warnings:
        print(f"{path}:{lineno}: warning: {message}")
    if warnings and args.strict:
        return EXIT_ERRORS
    print(f"ok: {len(cluster.nodes)} nodes, {cluster.vnodes} vnodes, {cluster.replicas} replica(s)"
          + (f", {len(warnings)} warning(s)" if warnings else ""))
    return EXIT_OK


def _cmd_lookup(cluster, args) -> int:
    results = lookup(cluster, args.key)
    if args.json:
        _dump(results)
        return EXIT_OK
    for r in results:
        print(f"{r['key']}  ->  {r['owner']}   replicas: {', '.join(r['replicas'])}")
    return EXIT_OK


def _cmd_balance(cluster, args) -> int:
    data = balance(cluster, args.keys, args.seed)
    gate_failed = args.max_imbalance is not None and data["max_ratio"] > args.max_imbalance
    if args.json:
        _dump({**data, "gate_failed": gate_failed})
    else:
        print(f"{data['keys']} sampled keys (seed {data['seed']})")
        print(_table(["node", "weight", "keys", "expected", "ratio", "deviation %"],
                     [[r["node"], r["weight"], r["keys"], r["expected"], f"{r['ratio']:.4f}",
                       f"{r['deviation_pct']:+.2f}"] for r in data["rows"]]))
        print(f"max ratio {data['max_ratio']:.4f} ({data['worst']})")
        if gate_failed:
            print(f"GATE FAILED: max ratio {data['max_ratio']:.4f} > {args.max_imbalance:.4f}")
    return EXIT_GATE if gate_failed else EXIT_OK


def _cmd_movement(cluster, args) -> int:
    if args.add is not None and any(n.name == args.add for n in cluster.nodes):
        raise UsageError(f"node '{args.add}' is already in the cluster")
    if args.remove is not None and not any(n.name == args.remove for n in cluster.nodes):
        raise UsageError(f"node '{args.remove}' is not in the cluster")
    data = movement(cluster, args.keys, args.seed, add=args.add, weight=args.weight,
                    remove=args.remove)
    gate_failed = data["misplaced"] > 0 or (
        args.max_excess is not None and data["excess"] > args.max_excess)
    if args.json:
        _dump({**data, "gate_failed": gate_failed})
    else:
        verb = "joins" if data["action"] == "add" else "leaves"
        print(f"node '{data['node']}' {verb}: {data['moved']} of {data['keys']} sampled keys move")
        print(f"observed {data['observed'] * 100:.2f}%   ideal {data['expected'] * 100:.2f}%   "
              f"excess {data['excess'] * 100:+.2f} pts")
        print(f"misplaced moves: {data['misplaced']}")
        if gate_failed:
            print("GATE FAILED")
    return EXIT_GATE if gate_failed else EXIT_OK


def _cmd_sweep(cluster, args) -> int:
    data = sweep(cluster, args.values, args.keys, args.seed, target=args.target)
    if args.json:
        _dump(data)
        return EXIT_OK
    print(f"{data['keys']} sampled keys (seed {data['seed']}); per-node vnodes= options are ignored here")
    print(_table(["vnodes", "points", "max ratio", "worst node", "target"],
                 [[r["vnodes"], r["points"], f"{r['max_ratio']:.4f}", r["worst"],
                   ("yes" if r["meets_target"] else "")] for r in data["rows"]]))
    if args.target is not None:
        if data["recommended"] is None:
            print(f"no tested vnode count reaches ratio {args.target:.4f}; try larger values")
        else:
            print(f"recommended: {data['recommended']} vnodes per unit weight")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    path = args.file
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"shardmap: cannot read {path}: {exc.strerror}", file=sys.stderr)
        return EXIT_USAGE
    except UnicodeDecodeError:
        print(f"shardmap: {path} is not UTF-8 text", file=sys.stderr)
        return EXIT_USAGE

    try:
        cluster, warnings = parse_cluster(text)
    except ConfigError as exc:
        for lineno, message in exc.problems:
            where = f"{path}:{lineno}" if lineno is not None else path
            print(f"{where}: error: {message}")
        return EXIT_ERRORS

    try:
        if args.command == "info":
            return _cmd_info(cluster, args)
        if args.command == "lint":
            return _cmd_lint(cluster, warnings, args, path)
        if args.command == "lookup":
            return _cmd_lookup(cluster, args)
        if args.command == "balance":
            return _cmd_balance(cluster, args)
        if args.command == "movement":
            return _cmd_movement(cluster, args)
        return _cmd_sweep(cluster, args)
    except UsageError as exc:
        print(f"shardmap: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
