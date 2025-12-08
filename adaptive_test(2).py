import time
import random
import statistics
from fluxdb import FluxDB

DOC_SIZES = [2000, 5000, 15000]   # scaling test sizes
QUERY_FIELD = "xp"
NUM_QUERIES = 200


# ------------------------------ Helpers ------------------------------

def timed(fn):
    start = time.time()
    fn()
    return (time.time() - start) * 1000  # ms


def insert_docs(db, n):
    def run():
        for i in range(n):
            db.insert({
                "username": f"user{i}",
                QUERY_FIELD: random.randint(1, 1000),
                "level": random.randint(1, 50)
            })
    return timed(run)


def run_match_query(db):
    val = random.randint(1, 1000)
    t0 = time.time()
    db.find({QUERY_FIELD: val})
    return (time.time() - t0) * 1000


def run_range_query(db):
    low = random.randint(1, 800)
    high = low + random.randint(5, 200)

    t0 = time.time()
    db.find({QUERY_FIELD: {"$gt": low, "$lt": high}})
    return (time.time() - t0) * 1000


def run_query_mix(db):
    match_times = []
    range_times = []

    for _ in range(NUM_QUERIES):
        if random.random() < 0.7:
            match_times.append(run_match_query(db))
        else:
            range_times.append(run_range_query(db))

    return match_times, range_times


# ------------------------------ Main Test ------------------------------

def run_mode(name, use_manual, use_adaptive, n_docs):
    print(f"\n=== 🚀 MODE {name}: {n_docs} docs ===")
    db = FluxDB()

    try: db._send_command("FLUSHDB")
    except: pass

    if use_manual:
        db.create_index(QUERY_FIELD, 0)

    db._send_command("CONFIG ADAPTIVE " + ("1" if use_adaptive else "0"))

    # Insert timing
    insert_ms = insert_docs(db, n_docs)
    print(f"   Insert time: {insert_ms:.2f} ms")

    # Query timing
    match_times, range_times = run_query_mix(db)

    db.close()

    return {
        "name": name,
        "n": n_docs,
        "insert_ms": insert_ms,
        "match_avg": statistics.mean(match_times) if match_times else 0,
        "range_avg": statistics.mean(range_times) if range_times else 0,
        "p95": statistics.quantiles(match_times + range_times, n=20)[18],
        "max": max(match_times + range_times)
    }


if __name__ == "__main__":
    results = []

    for n in DOC_SIZES:
        results.append(run_mode("MANUAL", True, False, n))
        results.append(run_mode("ADAPTIVE", False, True, n))

    print("\n=== 📊 FINAL SUMMARY ===")
    print("Mode       Docs    Insert(ms)  Match(ms)  Range(ms)  P95(ms)  Max(ms)")
    print("-----------------------------------------------------------------------")

    for r in results:
        print(f"{r['name']:<10} {r['n']:<7} {r['insert_ms']:>10.1f} "
              f"{r['match_avg']:>10.3f} {r['range_avg']:>10.3f} "
              f"{r['p95']:>8.3f} {r['max']:>8.3f}")
