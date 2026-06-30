from tessera import GPS, IMU, Vec3, flatten


def test_flatten_nested_to_dotted_columns():
    row = IMU(timestamp_ns=10, acceleration=Vec3(1.0, 2.0, 3.0),
              angular_velocity=Vec3(0.1, 0.2, 0.3)).row()
    assert row == {
        "timestamp_ns": 10,
        "acceleration.x": 1.0, "acceleration.y": 2.0, "acceleration.z": 3.0,
        "angular_velocity.x": 0.1, "angular_velocity.y": 0.2, "angular_velocity.z": 0.3,
    }


def test_flatten_flat_sensor():
    row = GPS(timestamp_ns=5, latitude=45.0, longitude=7.0, altitude=240.0,
              satellites=9, hdop=0.8).row()
    assert row["timestamp_ns"] == 5
    assert row["latitude"] == 45.0 and row["satellites"] == 9


def test_flatten_helper_matches_row():
    imu = IMU(1, Vec3(0, 0, 9.81), Vec3(0, 0, 0))
    assert flatten(imu) == imu.row()
