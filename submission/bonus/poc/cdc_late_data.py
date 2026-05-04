#!/usr/bin/env python3
"""
PoC — CDC Late-Arriving Data với Delta Lake + timestamp-aware MERGE.

Mô phỏng real-world scenario:
  - 100 trips đã ghi vào Silver table (completed)
  - 10 events đến muộn (late-arriving) từ CDC:
      * 5 events có source_ts OLDER → bị bỏ qua (không update)
      * 5 events có source_ts NEWER → update đúng
  - Dùng delta-rs merge() với predicate so sánh thời gian

Vấn đề: delta-rs <= 0.25.x không hỗ trợ condition trong
  when_matched_update_all(), nên timestamp comparison được thực hiện
  ở Python layer trước khi gọi merge.

Kiến trúc production sẽ dùng Spark MERGE condition hoặc Delta CDF.
"""

import polars as pl
from datetime import datetime, timedelta, timezone
from deltalake import DeltaTable, write_deltalake
import tempfile, shutil

TBL = tempfile.mkdtemp(suffix="_silver_trips")
BASE_TS = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)


def ts_str(offset_min: int) -> str:
    return (BASE_TS + timedelta(minutes=offset_min)).isoformat()


# ── 1. Initial batch: 100 trips ─────────────────────────────────
initial = pl.DataFrame({
    "trip_id": [f"t_{i}" for i in range(100)],
    "ts":      [ts_str(i) for i in range(100)],
    "status":  ["completed"] * 100,
    "fare":    [10.0 + i * 0.5 for i in range(100)],
})
write_deltalake(TBL, initial.to_arrow(), mode="overwrite")
print(f"✅ Initial: {len(initial)} trips written")

# ── 2. Late-arriving events ─────────────────────────────────────
#    Simulate 10 events: 5 OLDER ts, 5 NEWER ts
late = pl.DataFrame({
    "trip_id": [f"t_{i}" for i in [0, 5, 10, 20, 50] + [60, 70, 80, 90, 95]],
    "ts":      [ts_str(-5), ts_str(-1), ts_str(-3), ts_str(-2), ts_str(-10),
                ts_str(150), ts_str(155), ts_str(160), ts_str(165), ts_str(170)],
    "status":  ["cancelled", "cancelled", "refunded", "cancelled", "cancelled",
                "cancelled", "refunded", "cancelled", "cancelled", "refunded"],
    "fare":    [0.0, 0.0, 5.0, 0.0, 0.0,
                0.0, 5.0, 0.0, 0.0, 5.0],
})

print(f"\n📡 Late-arriving events ({len(late)}):")
for r in late.rows(named=True):
    print(f"   {r['trip_id']}: ts={r['ts']} status={r['status']} fare={r['fare']}")

# ── 3. MERGE với timestamp-aware logic ──────────────────────────
dt = DeltaTable(TBL)
before = pl.from_arrow(dt.to_pyarrow_table())

updates_applied = 0
skipped = 0

for r in late.rows(named=True):
    src_ts = r["ts"]
    tid = r["trip_id"]

    existing = before.filter(pl.col("trip_id") == tid)

    if existing.is_empty():
        src = pl.DataFrame([r])
        dt.merge(
            source=src.to_arrow(),
            predicate=f"tgt.trip_id = src.trip_id",
            source_alias="src",
            target_alias="tgt",
        ).when_not_matched_insert_all().execute()
        updates_applied += 1
        print(f"   📥 {tid}: inserted (new trip)")
    else:
        old_ts = existing.select("ts").item()
        if src_ts > old_ts:
            src = pl.DataFrame([r])
            dt.merge(
                source=src.to_arrow(),
                predicate=f"tgt.trip_id = src.trip_id",
                source_alias="src",
                target_alias="tgt",
            ).when_matched_update_all().execute()
            updates_applied += 1
            print(f"   🔄 {tid}: updated (NEWER ts: {src_ts} > {old_ts})")
        else:
            skipped += 1
            print(f"   ⏭️ {tid}: skipped (OLDER ts: {src_ts} <= {old_ts})")

# ── 4. Verify ───────────────────────────────────────────────────
after = pl.from_arrow(dt.to_pyarrow_table())

print("\n── Verification ──")
pass_count = 0

for tid in ["t_0", "t_5", "t_10", "t_20", "t_50"]:
    orig = before.filter(pl.col("trip_id") == tid)
    curr = after.filter(pl.col("trip_id") == tid)
    orig_vals = (orig.select("status").item(), orig.select("fare").item())
    curr_vals = (curr.select("status").item(), curr.select("fare").item())
    ok = orig_vals == curr_vals
    print(f"  {'✅' if ok else '❌'} {tid}: unchanged = {ok} "
          f"(was {orig_vals[0]}/{orig_vals[1]})")
    if ok:
        pass_count += 1

for tid in ["t_60", "t_70", "t_80", "t_90", "t_95"]:
    orig_fare = before.filter(pl.col("trip_id") == tid).select("fare").item()
    orig_status = before.filter(pl.col("trip_id") == tid).select("status").item()
    curr_fare = after.filter(pl.col("trip_id") == tid).select("fare").item()
    curr_status = after.filter(pl.col("trip_id") == tid).select("status").item()
    ok = orig_fare != curr_fare
    print(f"  {'✅' if ok else '❌'} {tid}: {orig_status}/{orig_fare} "
          f"→ {curr_status}/{curr_fare}")
    if ok:
        pass_count += 1

# ── 5. Summary ──────────────────────────────────────────────────
print(f"\n── PoC Result: {pass_count}/10 checks passed ──")
print(f"   {skipped} events ignored (older ts) — temporal integrity")
print(f"   {updates_applied} events applied (newer ts) — late data captured")
assert pass_count == 10, f"Expected 10/10, got {pass_count}/10"
print(f"   🎉 All checks passed! Temporal ordering maintained.")

shutil.rmtree(TBL)
