# Shardmap

**Plan a consistent-hash ring before you deploy it: see who owns each key, how evenly load lands, and how many keys move when a node joins or leaves.**

Shardmap reads one plain-text cluster file and works out everything you would otherwise guess at: weighted virtual nodes, replica sets, load balance against fair shares, key movement on membership change, and how many virtual nodes you actually need. It runs offline, uses only the Python standard library, and never touches a network.

## Features

- **Plain-text cluster file** with `vnodes`, `replicas` and `node NAME [weight=N] [vnodes=N]` directives and `#` comments.
- **Line-numbered lint** that reports every problem at once (`file:line: error: ...`), including duplicates, bad options, out-of-range numbers and unknown directives.
- **Strict lint mode** (`lint --strict`) that turns warnings, such as nodes with too few ring points, into failures.
- **Cluster summary** (`info`) with ring points, points share and fair share per node, in text or JSON.
- **Key lookup** (`lookup`) that shows the owning node and the clockwise replica set for any keys you name.
- **Deterministic ring** built on MD5 positions, so the same file always gives the same placement on every machine.
- **Weighted nodes**: a `weight=2` node gets twice the ring points and therefore about twice the keys.
- **Load balance report** (`balance`) that samples seeded synthetic keys and compares each node's load to its fair share.
- **CI balance gate** (`balance --max-imbalance RATIO`) that exits with code 3 when the busiest node exceeds the allowed ratio.
- **Membership-change simulation** (`movement --add NAME` / `--remove NAME`) that reports moved keys against the ideal share.
- **Misplaced-move check** that confirms keys only flow to a new node or away from a removed one, as consistent hashing requires.
- **Movement gate** (`movement --max-excess FRACTION`) for catching rings that move more keys than they should.
- **Virtual-node sweep** (`sweep --values ...`) that compares balance across vnode counts and recommends the smallest one that meets `--target`.
- **JSON output** on `info`, `lookup`, `balance`, `movement` and `sweep` for scripts and dashboards.
- **Reproducible sampling** with `--keys` and `--seed`, so reports can be checked and quoted.

## Quick start

The repository includes `examples/cluster.txt`, a three-node cache cluster where `cache-b` has twice the RAM:

```text
# Three cache nodes. cache-b has twice the RAM, so it takes twice the keys.
vnodes 160
replicas 2

node cache-a
node cache-b weight=2
node cache-c
```

Check it, then look at the layout:

```bash
python3 shardmap.py lint examples/cluster.txt
# ok: 3 nodes, 160 vnodes, 2 replica(s)

python3 shardmap.py info examples/cluster.txt
```

Find where a key lives, with its replicas:

```bash
python3 shardmap.py lookup examples/cluster.txt user:1042 session:abc
```

Lint a broken file. Every problem is reported with its line number and the command exits with code 1:

```bash
python3 shardmap.py lint examples/broken.txt
# examples/broken.txt:1: error: vnodes must be between 1 and 10000
# examples/broken.txt:2: error: replicas must be between 1 and 1 (the node count)
# examples/broken.txt:4: error: duplicate node 'cache-a' (first defined on line 3)
# examples/broken.txt:5: error: bad option 'b' (use weight=N with N in 1..100, or vnodes=N with N in 1..10000)
# examples/broken.txt:6: error: bad option 'weight=abc' (use weight=N with N in 1..100, or vnodes=N with N in 1..10000)
# examples/broken.txt:7: error: unknown directive 'bogus'
```

Check balance, and fail the build if the busiest node is more than 15% over its fair share:

```bash
python3 shardmap.py balance examples/cluster.txt --keys 100000 --max-imbalance 1.15
```

Simulate adding a fourth node and check that it takes roughly its fair share of keys:

```bash
python3 shardmap.py movement examples/cluster.txt --add cache-d --max-excess 0.05
```

Find the smallest virtual-node count that keeps the busiest node within 10% of fair:

```bash
python3 shardmap.py sweep examples/cluster.txt --values 16,64,256,1024 --target 1.10
```

## Cluster file format

| Line | Meaning |
| --- | --- |
| `vnodes N` | Default virtual nodes per unit of weight (1 to 10000, default 100). |
| `replicas N` | Number of distinct nodes in each key's replica set (default 1). |
| `node NAME [weight=W] [vnodes=V]` | A cache node. `W` is 1 to 100. `V` overrides the default for this node. |
| `# ...` | Comment, to the end of the line. |

Node names may use letters, digits, `.`, `_` and `-`. A node's ring points are `(vnodes) × weight`. The whole ring is capped at 1,000,000 points.

## Commands and exit codes

| Command | What it does |
| --- | --- |
| `info FILE [--json]` | Ring points and fair shares per node. |
| `lint FILE [--strict]` | Line-numbered errors and warnings. |
| `lookup FILE KEY... [--json]` | Owner and replica set for each key. |
| `balance FILE [--keys N] [--seed S] [--max-imbalance R] [--json]` | Sampled load against fair share. |
| `movement FILE (--add NAME [--weight W] \| --remove NAME) [--keys N] [--seed S] [--max-excess F] [--json]` | Keys moved on membership change. |
| `sweep FILE [--values LIST] [--target R] [--keys N] [--seed S] [--json]` | Balance across vnode counts, with a recommendation. |

Exit codes: `0` success, `1` the file has errors (or warnings under `--strict`), `2` usage or I/O problem, `3` a gate was exceeded (`--max-imbalance` or `--max-excess`, or a misplaced move was found).

## How to run

Requires Python 3.10 or newer. There is nothing to install for the tool itself:

```bash
python3 shardmap.py --help
python3 shardmap.py <command> <cluster-file> [options]
```

## How to test

The test suite uses pytest. Install it once, then run from the project root:

```bash
python3 -m pip install pytest
python3 -m pytest -q
```

The tests cover parsing and line-numbered errors, ring determinism, replica sets, the consistent-hashing guarantee on node addition, weighted balance, movement and misplaced-move checks, the vnode sweep, and the CLI exit codes and gates.

## Project layout

```text
shardmap.py          the planner and CLI (standard library only)
examples/cluster.txt  a valid three-node cluster
examples/broken.txt   a file with deliberate errors for lint
tests/                pytest suite
```

## Notes and limits

- Balance and movement figures come from sampled synthetic keys, so they are estimates. Use `--keys` for tighter numbers and `--seed` to reproduce a run.
- The ring is a model. It does not talk to real caches, and it does not model replication lag or failure timing.
