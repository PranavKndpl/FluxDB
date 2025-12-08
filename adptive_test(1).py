import time
import random
from statistics import mean
from fluxdb import FluxDB

NUM_DOCS = 5000
QUERY_FIELD = "xp"
NUM_QUERIES = 30
TARGET_VALUE = 500


def measure_inserts(db):
    start = time.time()
    for i in range(NUM_DOCS):
        db.insert({"name": f"player_{i}", QUERY_FIELD: random.randint(1, 1000)})
    return (time.time() - start) * 1000  # ms


def measure_queries(db):
    durations = []
    for _ in range(NUM_QUERIES):
        start = time.time()
        db.find({QUERY_FIELD: TARGET_VALUE})
        durations.append((time.time() - start) * 1000)
    return durations


def run_test(mode_name, use_manual_index, use_adaptive):
    print(f"\n\n=== 🧪 STRESS TEST: {mode_name} ===")

    db = FluxDB()
    try:
        db._send_command("FLUSHDB")
    except:
        pass

    if use_manual_index:
        print(f"   ➕ Manual index created on '{QUERY_FIELD}' (HASH)")
        db.create_index(QUERY_FIELD, 0)

    db._send_command(f"CONFIG ADAPTIVE {1 if use_adaptive else 0}")
    print("   🤖 Adaptive mode:", "ON" if use_adaptive else "OFF")

    # -------------------------------
    # INSERT TEST
    # -------------------------------
    print(f"   📥 Inserting {NUM_DOCS} documents...")
    insert_time = measure_inserts(db)
    print(f"      → Insert time: {insert_time:.2f} ms "
          f"({NUM_DOCS / (insert_time/1000):.0f} docs/sec)")

    # -------------------------------
    # QUERY TEST
    # -------------------------------
    print(f"   🔎 Running {NUM_QUERIES} queries on '{QUERY_FIELD}'...")
    query_times = measure_queries(db)

    print("      First 5 timings:", ", ".join(f"{t:.2f}ms" for t in query_times[:5]))

    avg_query = mean(query_times)
    worst_query = max(query_times)
    best_query = min(query_times)

    print(f"      → Avg query:   {avg_query:.3f} ms")
    print(f"      → Fastest:     {best_query:.3f} ms")
    print(f"      → Slowest:     {worst_query:.3f} ms")

    db.close()

    return {
        "mode": mode_name,
        "insert_ms": insert_time,
        "avg_query_ms": avg_query,
        "worst_query_ms": worst_query,
        "best_query_ms": best_query,
    }


if __name__ == "__main__":

    results = []

    results.append(run_test(
        "MANUAL INDEXING",
        use_manual_index=True,
        use_adaptive=False,
    ))

    results.append(run_test(
        "ADAPTIVE INDEXING",
        use_manual_index=False,
        use_adaptive=True,
    ))

    # -------------------------------
    # FINAL SUMMARY
    # -------------------------------
    print("\n\n=== 📊 FINAL SUMMARY ===")
    print(f"{'Mode':<20} {'Insert(ms)':>12} {'Avg Query(ms)':>15} {'Worst(ms)':>12}")
    print("-" * 62)

    for r in results:
        print(f"{r['mode']:<20} "
              f"{r['insert_ms']:>12.2f} "
              f"{r['avg_query_ms']:>15.3f} "
              f"{r['worst_query_ms']:>12.3f}")
