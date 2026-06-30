from tessera import GPS, IMU, Lake, Vec3

T0 = 1_700_000_000_000_000_000


def test_align_zero_order_hold(tmp_path):
    lake = Lake(tmp_path / "lake")
    # IMU at 100 Hz, GPS at 5 Hz, same start
    imu = [IMU(T0 + i * 10_000_000, Vec3(0.01 * i, 0.0, 9.81), Vec3(0, 0, 0))
           for i in range(100)]
    gps = [GPS(T0 + i * 200_000_000, latitude=45.0 + i * 0.001,
               longitude=7.0, altitude=240.0) for i in range(5)]
    lake.write("run01", "imu", imu)
    lake.write("run01", "gps", gps)

    out = lake.align("run01", ["imu", "gps"], hz=50,
                     columns={"imu": ["acceleration.x"], "gps": ["latitude"]})
    d = out.to_pydict()

    # fixed-frequency grid: 20 ms step -> no gaps
    ts = d["timestamp_ns"]
    steps = {b - a for a, b in zip(ts, ts[1:])}
    assert steps == {20_000_000}

    # GPS holds its value until the next fix at t=200ms (45.001)
    assert d["gps.latitude"][0] == 45.0
    idx_200ms = ts.index(T0 + 200_000_000)
    assert d["gps.latitude"][idx_200ms] == 45.001
    assert d["gps.latitude"][idx_200ms - 1] == 45.0  # still held just before

    # IMU (fast) is present on every tick
    assert all(v is not None for v in d["imu.acceleration.x"])
