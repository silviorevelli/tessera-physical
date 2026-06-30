from tessera import Lake
from tessera.ingest import ingest_csv, read_csv

CSV = "t,acc_x,acc_y,acc_z\n0,0.1,0.0,9.81\n1,7.5,0.0,9.81\n2,0.2,0.0,9.81\n"
RENAME = {"acc_x": "acceleration.x", "acc_y": "acceleration.y", "acc_z": "acceleration.z"}


def test_read_csv_normalizes_time_and_renames(tmp_path):
    p = tmp_path / "imu.csv"
    p.write_text(CSV)
    table = read_csv(p, timestamp="t", unit="s", rename=RENAME)
    assert "timestamp_ns" in table.column_names
    assert "acceleration.x" in table.column_names
    # 1 second -> 1e9 ns
    assert table.column("timestamp_ns").to_pylist() == [0, 1_000_000_000, 2_000_000_000]


def test_ingest_csv_writes_into_lake(tmp_path):
    p = tmp_path / "imu.csv"
    p.write_text(CSV)
    lake = Lake(tmp_path / "lake")
    parts = ingest_csv(lake, "run01", "imu", p, timestamp="t", unit="s", rename=RENAME)

    assert len(parts) == 1
    assert lake.sequences("imu") == ["run01"]
    hit = lake.find("imu", "acceleration.x > 5")
    assert hit.num_rows == 1
    assert hit.column("timestamp_ns").to_pylist() == [1_000_000_000]
