import threading
import time

import pytest

from tessera import IMU, Lake, TesseraClient, TesseraServer, Vec3

API_KEY = "test-key"


def _imu(t, ax):
    return IMU(t, Vec3(ax, 0.0, 9.81), Vec3(0.0, 0.0, 0.0))


@pytest.fixture
def server(tmp_path):
    srv = TesseraServer(Lake(tmp_path / "lake"),
                        location="grpc://127.0.0.1:0", api_keys={API_KEY})
    threading.Thread(target=srv.serve, daemon=True).start()
    time.sleep(0.3)
    yield srv, f"grpc://127.0.0.1:{srv.port}"
    srv.shutdown()


def test_roundtrip_write_and_query(server):
    _, location = server
    client = TesseraClient(location, api_key=API_KEY)
    assert client.ping() == "pong"

    client.write("run01", "imu", [_imu(10, 0.1), _imu(20, 9.0)])
    assert client.topics() == ["imu"]
    assert client.sequences("imu") == ["run01"]

    hits = client.find("imu", "acceleration.x > 5")
    assert hits.num_rows == 1
    assert hits.column("acceleration.x").to_pylist() == [9.0]


def test_two_clients_share_one_lake(server):
    _, location = server
    writer = TesseraClient(location, api_key=API_KEY)
    reader = TesseraClient(location, api_key=API_KEY)
    writer.write("run01", "imu", [_imu(1, 0.0)])
    writer.write("run02", "imu", [_imu(2, 0.0)])
    assert reader.sequences("imu") == ["run01", "run02"]


def test_bad_api_key_rejected(server):
    import pyarrow.flight as fl
    _, location = server
    with pytest.raises(fl.FlightError):
        TesseraClient(location, api_key="wrong").ping()
