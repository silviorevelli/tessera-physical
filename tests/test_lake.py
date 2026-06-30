import pytest

from tessera import IMU, Lake, Vec3


def _imu(t, ax):
    return IMU(t, Vec3(ax, 0.0, 9.81), Vec3(0.0, 0.0, 0.0))


@pytest.fixture
def lake(tmp_path):
    lk = Lake(tmp_path / "lake")
    lk.write("run01", "imu", [_imu(10, 0.1), _imu(20, 0.2), _imu(30, 9.0)])
    lk.write("run02", "imu", [_imu(40, 0.3), _imu(50, 0.4)])
    return lk


def test_topics_and_sequences(lake):
    assert lake.topics() == ["imu"]
    assert lake.sequences("imu") == ["run01", "run02"]


def test_find_by_physical_value(lake):
    t = lake.find("imu", "acceleration.x > 5")
    assert t.num_rows == 1
    assert t.column("acceleration.x").to_pylist() == [9.0]
    assert t.column("sequence").to_pylist() == ["run01"]


def test_find_all_ordered_by_time(lake):
    t = lake.find("imu")
    assert t.num_rows == 5
    ts = t.column("timestamp_ns").to_pylist()
    assert ts == sorted(ts)


def test_sequence_filter_prunes(lake):
    t = lake.find("imu", sequences=["run02"])
    assert set(t.column("sequence").to_pylist()) == {"run02"}
    assert t.num_rows == 2


def test_catalog_groups_by_sequence(lake):
    cat = lake.catalog("imu", "acceleration.x > 5").to_pydict()
    assert cat["sequence"] == ["run01"]
    assert cat["matches"] == [1]


def test_append_creates_multiple_parts(tmp_path):
    lk = Lake(tmp_path / "lake")
    lk.write("run01", "imu", [_imu(1, 0.0)])
    lk.write("run01", "imu", [_imu(2, 0.0)])  # append, not overwrite
    part_dir = tmp_path / "lake" / "imu" / "sequence=run01"
    assert len(list(part_dir.glob("*.parquet"))) == 2
    assert lk.find("imu").num_rows == 2


def test_find_stream_yields_batches(lake):
    rows = sum(b.num_rows for b in lake.find_stream("imu"))
    assert rows == 5


def test_write_empty_raises(tmp_path):
    with pytest.raises(ValueError):
        Lake(tmp_path / "lake").write("run01", "imu", [])
