# stoolap-python

High-performance Python driver for [Stoolap](https://stoolap.io) embedded SQL database. Built with [PyO3](https://pyo3.rs) for native Rust performance with both sync and async APIs.

## Performance

**53 out of 53 benchmark wins** against Python's built-in `sqlite3` on 10,000 rows:

| Category | Highlights |
|----------|-----------|
| **Point Queries** | SELECT by ID: 1.5x, SELECT by index: 1.4-2.0x |
| **Complex Queries** | SELECT complex: 4.8x, Scalar subquery: 19.5x |
| **Aggregations** | GROUP BY: 24.8x, COUNT DISTINCT: 207x |
| **Joins** | INNER JOIN: 1.1x, LEFT JOIN: 1.6x, Self JOIN: 1.3x |
| **Subqueries** | IN subquery: 12.7x, NOT EXISTS: 42.8x, Nested 3-level: 16.3x |
| **Window Functions** | ROW_NUMBER: 5.5x, PARTITION BY: 4.2x, ROWS frame: 2.7x |
| **Write Operations** | DELETE complex: 133x, UPDATE complex: 6.7x |

Run the benchmark yourself: `python benchmark.py`

## Installation

```bash
pip install stoolap-python
```

## Quick Start

```python
from stoolap import Database

# In-memory database
db = Database.open(":memory:")

# exec() runs one or more DDL/DML statements (no parameters)
db.exec("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    CREATE INDEX idx_users_name ON users(name);
""")

# execute() runs a single statement with parameters, returns rows affected
db.execute(
    "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
    [1, "Alice", "alice@example.com"],
)

# Named parameters (:key)
db.execute(
    "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
)

# query() returns a list of dicts
users = db.query("SELECT * FROM users ORDER BY id")
# [{"id": 1, "name": "Alice", "email": "alice@example.com"}, ...]

# query_one() returns a single dict or None
user = db.query_one("SELECT * FROM users WHERE id = $1", [1])
# {"id": 1, "name": "Alice", "email": "alice@example.com"}

# query_raw() returns columnar format (faster for large results)
raw = db.query_raw("SELECT id, name FROM users ORDER BY id")
# {"columns": ["id", "name"], "rows": [[1, "Alice"], [2, "Bob"]]}

db.close()
```

## Prepared Statements

Parse SQL once, execute many times with different parameters:

```python
insert = db.prepare("INSERT INTO users (id, name) VALUES ($1, $2)")
insert.execute([1, "Alice"])
insert.execute([2, "Bob"])

# Batch execution (auto-wrapped in a transaction)
insert.execute_batch([
    [3, "Charlie"],
    [4, "Diana"],
])

# Prepared queries
lookup = db.prepare("SELECT * FROM users WHERE id = $1")
user = lookup.query_one([1])       # Single row as dict or None
rows = lookup.query([1])           # All rows as list of dicts
raw  = lookup.query_raw([1])       # Columnar format

# Named parameters also work with prepared statements
lookup = db.prepare("SELECT * FROM users WHERE id = :id")
user = lookup.query_one({"id": 1})
```

## Transactions

```python
# Context manager (auto-commit on clean exit, auto-rollback on exception)
with db.begin() as tx:
    tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])
    tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [2, "Bob"])

# Manual control
tx = db.begin()
try:
    tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])
    tx.commit()
except:
    tx.rollback()
    raise
```

Transactions support `execute()`, `query()`, `query_one()`, `query_raw()`, and `execute_batch()` with both positional (`$1, $2`) and named (`:key`) parameters.

## Batch Execution

Execute the same statement with multiple parameter sets, auto-wrapped in a transaction:

```python
# On Database
changes = db.execute_batch(
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    [[1, "Alice"], [2, "Bob"], [3, "Charlie"]],
)
# changes == 3

# On PreparedStatement (reuses cached plan)
stmt = db.prepare("INSERT INTO users (id, name) VALUES ($1, $2)")
changes = stmt.execute_batch([[4, "Diana"], [5, "Eve"]])
```

## Async API

All methods release the GIL and run on a thread executor:

```python
from stoolap import AsyncDatabase

db = await AsyncDatabase.open(":memory:")

await db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
await db.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])

rows = await db.query("SELECT * FROM users")

# Async transactions
async with await db.begin() as tx:
    await tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [2, "Bob"])

# Async prepared statements
stmt = db.prepare("SELECT * FROM users WHERE id = $1")
user = await stmt.query_one([1])

await db.close()
```

## Error Handling

All database errors raise `StoolapError`:

```python
from stoolap import Database, StoolapError

db = Database.open(":memory:")
try:
    db.query("SELECT * FROM nonexistent_table")
except StoolapError as e:
    print(f"Database error: {e}")
```

## Persistence

```python
# File-based database (data persists across restarts)
db = Database.open("file:///path/to/mydata")

# Relative paths also work
db = Database.open("./mydata")
```

### Configuration Options

Pass options as query parameters in the DSN:

```python
# Max durability
db = Database.open("file:///path/to/mydata?sync=full")

# Max throughput (less durable)
db = Database.open("file:///path/to/mydata?sync=none&snapshot_interval=600")
```

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `sync` | `none`, `normal`, `full` | `normal` | Durability level (`full` = fsync every write) |
| `snapshot_interval` | seconds | `300` | Auto-snapshot interval |
| `keep_snapshots` | count | `5` | Number of snapshots to retain |
| `compression` | `on`, `off` | `on` | Enable WAL + snapshot compression (LZ4) |
| `wal_compression` | `on`, `off` | `on` | WAL compression only |
| `snapshot_compression` | `on`, `off` | `on` | Snapshot compression only |
| `compression_threshold` | bytes | `64` | Minimum data size before compression |
| `sync_interval_ms` | milliseconds | `10` | Background sync interval |
| `wal_buffer_size` | bytes | `65536` | WAL write buffer size |
| `wal_flush_trigger` | bytes | `32768` | WAL size before flush |
| `wal_max_size` | bytes | `67108864` | WAL size before forced snapshot |
| `commit_batch_size` | count | `100` | Commits batched before flush |
| `cleanup` | `on`, `off` | `on` | Enable background cleanup thread |
| `cleanup_interval` | seconds | `60` | Interval between cleanup runs |
| `deleted_row_retention` | seconds | `300` | Keep deleted rows before permanent removal |
| `transaction_retention` | seconds | `3600` | Keep stale transaction metadata |

## Type Mapping

| Python | Stoolap | Notes |
|--------|---------|-------|
| `int` | `INTEGER` | 64-bit signed |
| `float` | `FLOAT` | 64-bit double |
| `str` | `TEXT` | UTF-8 |
| `bool` | `BOOLEAN` | |
| `None` | `NULL` | |
| `datetime.datetime` | `TIMESTAMP` | Converted to/from UTC |
| `dict` / `list` | `JSON` | Serialized via `json.dumps` |
| `Vector` | `VECTOR(N)` | `list[float]` on output |

## Vector Similarity Search

Store embeddings and perform k-NN similarity search using HNSW indexes:

```python
from stoolap import Database, Vector

db = Database.open(":memory:")

# Create a table with a VECTOR column
db.exec("""
    CREATE TABLE documents (
        id INTEGER PRIMARY KEY,
        title TEXT,
        embedding VECTOR(3)
    );
    CREATE INDEX idx_emb ON documents(embedding) USING HNSW WITH (metric = 'cosine');
""")

# Insert vectors using the Vector wrapper
db.execute(
    "INSERT INTO documents VALUES ($1, $2, $3)",
    [1, "Hello world", Vector([0.1, 0.2, 0.3])],
)
db.execute(
    "INSERT INTO documents VALUES ($1, $2, $3)",
    [2, "Goodbye world", Vector([0.9, 0.1, 0.0])],
)

# k-NN search: find 5 nearest neighbors
results = db.query(
    "SELECT id, title, VEC_DISTANCE_COSINE(embedding, '[0.1, 0.2, 0.3]') AS dist "
    "FROM documents ORDER BY dist LIMIT 5"
)

# Read vectors back as list[float]
row = db.query_one("SELECT embedding FROM documents WHERE id = 1")
emb = row["embedding"]  # [0.1, 0.2, 0.3]
```

### Distance Functions

| Function | Description |
|----------|-------------|
| `VEC_DISTANCE_L2(a, b)` | Euclidean distance |
| `VEC_DISTANCE_COSINE(a, b)` | Cosine distance (1 - similarity) |
| `VEC_DISTANCE_IP(a, b)` | Negative inner product |

### Vector Utilities

| Function | Description |
|----------|-------------|
| `VEC_DIMS(v)` | Number of dimensions |
| `VEC_NORM(v)` | L2 norm (magnitude) |
| `VEC_TO_TEXT(v)` | Convert to string `[1.0, 2.0, 3.0]` |

### HNSW Index Options

```sql
CREATE INDEX idx ON table(column) USING HNSW WITH (metric = 'cosine');
```

Supported metrics: `l2` (default), `cosine`, `ip` (inner product).

## Features

Stoolap is a full-featured embedded SQL database:

- **MVCC Transactions** with snapshot isolation
- **Cost-based query optimizer** with adaptive execution
- **Parallel query execution** (filter, join, sort, distinct)
- **JOINs**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL
- **Subqueries**: scalar, EXISTS, IN, NOT IN, ANY/ALL, correlated
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE
- **CTEs**: WITH and WITH RECURSIVE
- **Aggregations**: GROUP BY, HAVING, ROLLUP, CUBE, GROUPING SETS
- **Vector similarity search** with HNSW indexes (L2, cosine, inner product)
- **Indexes**: B-tree, Hash, Bitmap (auto-selected), HNSW, multi-column composite
- **110+ built-in functions**: string, math, date/time, JSON, vector, aggregate
- **WAL + snapshots** for crash recovery
- **Semantic query caching** with predicate subsumption

## Building from Source

Requires [Rust](https://rustup.rs) (stable) and Python >= 3.9.

```bash
git clone https://github.com/stoolap/stoolap-python.git
cd stoolap-python
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest pytest-asyncio
maturin develop --release
pytest
```

## License

Apache-2.0
