"""Same-instance (local) replication tests

A replication without ``dst_host`` writes records directly through the storage
engine into ``dst_bucket`` of the same instance, so it needs neither a URL nor
a token. Replication is asynchronous: the tests poll until the destination
reaches the expected state instead of sleeping for a fixed time.
"""

import os
import time

import pytest

LABEL_HEADER_PREFIX = "x-reduct-label-"
REPLICATION_TIMEOUT = 15  # seconds to wait for the asynchronous replication
POLL_INTERVAL = 0.1  # seconds between two polls
SETTLE_TIME = 0.5  # seconds to wait before checking that something is NOT replicated

# A local replication has an empty or no destination host
LOCAL_HOST_SETTINGS = pytest.mark.parametrize(
    "host_settings", [{}, {"dst_host": ""}], ids=["dst_host_omitted", "dst_host_empty"]
)


def wait_until(condition, description, timeout=REPLICATION_TIMEOUT):
    """Poll the condition until it returns something truthy and return it"""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if result:
            return result

        if time.monotonic() >= deadline:
            raise AssertionError(f"Timeout of {timeout} s: {description}")
        time.sleep(POLL_INTERVAL)


def labels_of(resp):
    """Get the labels of a record from the response of a read request"""
    return {
        name.lower()[len(LABEL_HEADER_PREFIX) :]: value
        for name, value in resp.headers.items()
        if name.lower().startswith(LABEL_HEADER_PREFIX)
    }


def assert_same_record(actual, expected):
    """Check that two read responses carry the same record"""
    assert actual.status_code == 200
    assert actual.content == expected.content
    assert actual.headers["x-reduct-time"] == expected.headers["x-reduct-time"]
    assert actual.headers["content-type"] == expected.headers["content-type"]
    assert labels_of(actual) == labels_of(expected)


class StoreClient:
    """HTTP helper of one instance which removes everything a test has created"""

    def __init__(self, base_url, session):
        self.base_url = base_url
        self.session = session
        self._replications = []
        self._buckets = []

    def create_bucket(self, name):
        """Create a bucket which is removed after the test"""
        resp = self.session.post(f"{self.base_url}/b/{name}")
        assert resp.status_code == 200, resp.headers.get("x-reduct-error")
        return self.track_bucket(name)

    def track_bucket(self, name):
        """Remove a bucket after the test, e.g. one a replication is going to create"""
        self._buckets.append(name)
        return name

    def create_replication(self, name, src_bucket, dst_bucket, **settings):
        """Create a replication and return the response.

        It is a local replication unless `dst_host` is set in the settings.
        """
        resp = self.session.post(
            f"{self.base_url}/replications/{name}",
            json={"src_bucket": src_bucket, "dst_bucket": dst_bucket, **settings},
        )
        if resp.status_code == 200:
            self._replications.append(name)
        return resp

    def update_replication(self, name, src_bucket, dst_bucket, **settings):
        """Update a replication and return the response"""
        return self.session.put(
            f"{self.base_url}/replications/{name}",
            json={"src_bucket": src_bucket, "dst_bucket": dst_bucket, **settings},
        )

    def get_replication(self, name):
        """Get the settings, info and diagnostics of a replication"""
        resp = self.session.get(f"{self.base_url}/replications/{name}")
        assert resp.status_code == 200, resp.headers.get("x-reduct-error")
        return resp.json()

    def set_mode(self, name, mode):
        """Set the mode of a replication"""
        resp = self.session.patch(
            f"{self.base_url}/replications/{name}/mode", json={"mode": mode}
        )
        assert resp.status_code == 200, resp.headers.get("x-reduct-error")

    def write(self, bucket, entry, ts, data=b"", content_type=None, labels=None):
        """Write a record"""
        headers = {f"{LABEL_HEADER_PREFIX}{k}": v for k, v in (labels or {}).items()}
        if content_type:
            headers["content-type"] = content_type

        resp = self.session.post(
            f"{self.base_url}/b/{bucket}/{entry}?ts={ts}", data=data, headers=headers
        )
        assert resp.status_code == 200, resp.headers.get("x-reduct-error")

    def update_labels(self, bucket, entry, ts, labels):
        """Update labels of a record"""
        headers = {f"{LABEL_HEADER_PREFIX}{k}": v for k, v in labels.items()}
        resp = self.session.patch(
            f"{self.base_url}/b/{bucket}/{entry}?ts={ts}", headers=headers
        )
        assert resp.status_code == 200, resp.headers.get("x-reduct-error")

    def read(self, bucket, entry, ts):
        """Read a record and return the response"""
        return self.session.get(f"{self.base_url}/b/{bucket}/{entry}?ts={ts}")

    def wait_for_record(self, bucket, entry, ts, labels=None):
        """Wait until the record can be read (and has the labels) and return it"""

        def _record():
            resp = self.read(bucket, entry, ts)
            if resp.status_code != 200:
                return None
            if labels and not labels.items() <= labels_of(resp).items():
                return None
            return resp

        with_labels = f" with labels {labels}" if labels else ""
        return wait_until(_record, f"record {bucket}/{entry}?ts={ts}{with_labels}")

    def wait_for_record_count(self, bucket, entry, count):
        """Wait until the entry has the number of records"""

        def _reached():
            resp = self.session.get(f"{self.base_url}/b/{bucket}")
            return resp.status_code == 200 and any(
                e["name"] == entry and e["record_count"] == count
                for e in resp.json()["entries"]
            )

        wait_until(_reached, f"{count} records in {bucket}/{entry}")

    def wait_for_pending_records(self, name, count=0):
        """Wait until the replication has the number of records to replicate.

        Notifications about writes are queued, so call it after the destination
        has shown that the replication has processed the writes.
        """

        def _reached():
            return self.get_replication(name)["info"]["pending_records"] == count

        wait_until(_reached, f"{count} pending records of replication '{name}'")

    def cleanup(self):
        """Remove replications before buckets, so they don't write into removed ones"""
        failures = []
        for path, names in (("replications", self._replications), ("b", self._buckets)):
            for name in reversed(names):
                resp = self.session.delete(f"{self.base_url}/{path}/{name}")
                if resp.status_code not in (200, 404):
                    error = resp.headers.get("x-reduct-error")
                    failures.append(f"{path}/{name}: {resp.status_code} {error}")
        assert not failures, f"Failed to clean up: {failures}"


@pytest.fixture(name="store")
def _make_store(base_url, session):
    """Helper to work with the instance; removes what a test has created"""
    store = StoreClient(base_url, session)
    yield store
    store.cleanup()


@LOCAL_HOST_SETTINGS
def test__create_local_replication_ok(
    base_url, session, store, bucket_name, replication_name, host_settings
):
    """Should create a replication without dst_host and dst_token"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    when = {"$eq": ["&key1", "value1"]}

    resp = store.create_replication(
        replication_name,
        src,
        dst,
        entries=["entry1", "entry2"],
        when=when,
        dst_prefix="robot-1",
        **host_settings,
    )
    assert resp.status_code == 200

    replication = store.get_replication(replication_name)
    assert replication["settings"] == {
        "src_bucket": src,
        "dst_bucket": dst,
        "dst_host": "",
        "dst_prefix": "robot-1",
        "dst_token": None,
        "entries": ["entry1", "entry2"],
        "when": when,
        "mode": "enabled",
        "compression": "none",
    }
    assert replication["info"]["name"] == replication_name
    assert replication["info"]["mode"] == "enabled"
    assert replication["info"]["is_provisioned"] is False
    assert replication["info"]["pending_records"] == 0

    resp = session.get(f"{base_url}/replications")
    assert resp.status_code == 200
    assert replication_name in [r["name"] for r in resp.json()["replications"]]


def test__create_local_replication_with_invalid_src_bucket(
    store, bucket_name, replication_name
):
    """Should not create a local replication with a missing source bucket"""
    dst = store.create_bucket(f"{bucket_name}_dst")

    resp = store.create_replication(replication_name, f"{bucket_name}_missing", dst)
    assert resp.status_code == 404


@LOCAL_HOST_SETTINGS
def test__create_local_replication_with_the_same_bucket(
    base_url, session, store, bucket_name, replication_name, host_settings
):
    """Should not create a local replication which reads and writes the same bucket"""
    bucket = store.create_bucket(bucket_name)

    resp = store.create_replication(replication_name, bucket, bucket, **host_settings)
    assert resp.status_code == 422
    assert resp.headers["x-reduct-error"]

    resp = session.get(f"{base_url}/replications/{replication_name}")
    assert resp.status_code == 404


def test__create_local_replication_with_a_loop(
    base_url, session, store, bucket_name, replication_name
):
    """Should not create a local replication which sends records back to their source"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")

    resp = store.create_replication(f"{replication_name}_ab", a, b)
    assert resp.status_code == 200

    resp = store.create_replication(f"{replication_name}_ba", b, a)
    assert resp.status_code == 422
    assert "loop" in resp.headers["x-reduct-error"].lower()

    resp = session.get(f"{base_url}/replications/{replication_name}_ba")
    assert resp.status_code == 404

    settings = store.get_replication(f"{replication_name}_ab")["settings"]
    assert (settings["src_bucket"], settings["dst_bucket"]) == (a, b)


def test__create_local_replication_with_a_loop_of_three_buckets(
    base_url, session, store, bucket_name, replication_name
):
    """Should not create a local replication which closes a chain of replications"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")
    c = store.create_bucket(f"{bucket_name}_c")

    assert store.create_replication(f"{replication_name}_ab", a, b).status_code == 200
    assert store.create_replication(f"{replication_name}_bc", b, c).status_code == 200

    resp = store.create_replication(f"{replication_name}_ca", c, a)
    assert resp.status_code == 422
    assert "loop" in resp.headers["x-reduct-error"].lower()

    resp = session.get(f"{base_url}/replications/{replication_name}_ca")
    assert resp.status_code == 404


def test__update_local_replication_with_a_loop(store, bucket_name, replication_name):
    """Should not update a local replication if this creates a loop"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")
    c = store.create_bucket(f"{bucket_name}_c")
    d = store.create_bucket(f"{bucket_name}_d")

    assert store.create_replication(f"{replication_name}_ab", a, b).status_code == 200
    assert store.create_replication(f"{replication_name}_cd", c, d).status_code == 200

    resp = store.update_replication(f"{replication_name}_cd", b, a)
    assert resp.status_code == 422
    assert "loop" in resp.headers["x-reduct-error"].lower()

    # a rejected update keeps the replication as it was
    settings = store.get_replication(f"{replication_name}_cd")["settings"]
    assert (settings["src_bucket"], settings["dst_bucket"]) == (c, d)


def test__update_local_replication_direction(store, bucket_name, replication_name):
    """Should let a local replication change its direction and replicate that way"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")
    assert store.create_replication(replication_name, a, b).status_code == 200

    # the old settings of the replication must not count as a loop with the new ones
    resp = store.update_replication(replication_name, b, a)
    assert resp.status_code == 200

    settings = store.get_replication(replication_name)["settings"]
    assert (settings["src_bucket"], settings["dst_bucket"]) == (b, a)
    assert settings["dst_host"] == ""

    store.write(b, "entry", 1000, b"reversed")
    resp = store.wait_for_record(a, "entry", 1000)
    assert resp.content == b"reversed"


def test__create_local_replications_without_loops(
    base_url, session, store, bucket_name, replication_name
):
    """Should create local replications which fan out and join again"""
    buckets = {name: store.create_bucket(f"{bucket_name}_{name}") for name in "abcd"}
    edges = ["ab", "ac", "bd", "cd"]

    for edge in edges:
        resp = store.create_replication(
            f"{replication_name}_{edge}", buckets[edge[0]], buckets[edge[1]]
        )
        assert resp.status_code == 200

    resp = session.get(f"{base_url}/replications")
    assert resp.status_code == 200
    names = [r["name"] for r in resp.json()["replications"]]
    for edge in edges:
        assert f"{replication_name}_{edge}" in names


def test__http_replication_is_not_a_part_of_a_local_loop(
    store, bucket_name, replication_name
):
    """Should create an HTTP replication even if local replications go the other way"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")
    assert store.create_replication(f"{replication_name}_ab", a, b).status_code == 200

    resp = store.create_replication(
        f"{replication_name}_ba", b, a, dst_host="http://localhost:9000"
    )
    assert resp.status_code == 200


def test__http_replication_keeps_dst_host_and_masks_dst_token(
    store, bucket_name, replication_name
):
    """Should keep creating an HTTP replication when dst_host is set"""
    src = store.create_bucket(f"{bucket_name}_src")

    resp = store.create_replication(
        replication_name,
        src,
        "dst_bucket",
        dst_host="http://localhost:9000",
        dst_token="secret-token",
    )
    assert resp.status_code == 200

    settings = store.get_replication(replication_name)["settings"]
    assert settings["dst_host"] == "http://localhost:9000"
    assert settings["dst_token"] is None


def test__create_replication_with_invalid_dst_host(
    store, bucket_name, replication_name
):
    """Should not treat a broken destination host as a local replication"""
    src = store.create_bucket(f"{bucket_name}_src")

    resp = store.create_replication(
        replication_name, src, "dst_bucket", dst_host="BROKEN URL"
    )
    assert resp.status_code == 422


def test__local_replication_copies_records(store, bucket_name, replication_name):
    """Should replicate records with the same timestamp, body, content type and labels"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    assert store.create_replication(replication_name, src, dst).status_code == 200

    records = [
        # entry, timestamp, body, content type, labels
        ("entry_a", 1000, b"plain body", None, {}),
        ("entry_a", 2000, b"hello", "text/plain", {"camera": "front", "tags": "a,b"}),
        (
            "entry_a",
            3000,
            b'{"temperature": 21.5}',
            "application/json",
            {"sensor": "s-1", "quality": "high"},
        ),
        ("entry_b", 1000, b"", "text/plain", {"empty": "true"}),
        # more than one chunk of data
        ("entry_b", 2000, os.urandom(1_500_000), "application/x-custom", {"big": "y"}),
    ]
    for entry, ts, body, content_type, labels in records:
        store.write(src, entry, ts, body, content_type, labels)

    for entry, ts, body, content_type, labels in records:
        replicated = store.wait_for_record(dst, entry, ts)
        assert_same_record(replicated, store.read(src, entry, ts))
        assert replicated.content == body
        assert replicated.headers["x-reduct-time"] == str(ts)
        assert replicated.headers["content-type"] == (
            content_type or "application/octet-stream"
        )

    store.wait_for_pending_records(replication_name)


def test__local_replication_reports_diagnostics(store, bucket_name, replication_name):
    """Should report an active replication and count replicated records"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    assert store.create_replication(replication_name, src, dst).status_code == 200

    for ts in (1000, 2000):
        store.write(src, "entry", ts, b"data")
    for ts in (1000, 2000):
        store.wait_for_record(dst, "entry", ts)

    def _counted():
        replication = store.get_replication(replication_name)
        counted = replication["diagnostics"]["hourly"]["ok"] > 0
        drained = replication["info"]["pending_records"] == 0
        return replication if counted and drained else None

    replication = wait_until(_counted, "replicated records in the diagnostics")
    assert replication["diagnostics"]["hourly"]["errored"] == 0
    assert replication["diagnostics"]["hourly"]["errors"] == {}
    assert replication["info"]["is_active"] is True


@pytest.mark.parametrize(
    "dst_prefix, dst_entry",
    [
        ("robot-1", "robot-1/camera/front"),
        ("/robot-1/", "robot-1/camera/front"),
        ("robot-1/site-a", "robot-1/site-a/camera/front"),
    ],
)
def test__local_replication_prepends_dst_prefix(
    base_url, session, store, bucket_name, replication_name, dst_prefix, dst_entry
):
    """Should write records under the destination prefix like an HTTP replication"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    resp = store.create_replication(replication_name, src, dst, dst_prefix=dst_prefix)
    assert resp.status_code == 200

    store.write(src, "camera/front", 1000, b"frame-1", labels={"frame": "1"})
    store.write(src, "camera/front", 2000, b"frame-2", labels={"frame": "2"})

    for ts, frame in ((1000, "1"), (2000, "2")):
        resp = store.wait_for_record(dst, dst_entry, ts)
        assert resp.content == f"frame-{frame}".encode()
        assert labels_of(resp) == {"frame": frame}
    store.wait_for_pending_records(replication_name)

    # nothing is written under the source name
    assert store.read(dst, "camera/front", 1000).status_code == 404
    resp = session.get(f"{base_url}/b/{dst}")
    assert resp.status_code == 200
    names = {entry["name"] for entry in resp.json()["entries"]}
    assert dst_entry in names
    assert "camera/front" not in names


def test__local_replication_propagates_label_updates(
    store, bucket_name, replication_name
):
    """Should update labels of the destination record if the source record is updated"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    assert store.create_replication(replication_name, src, dst).status_code == 200

    store.write(
        src, "entry", 1000, b"some_data", "text/plain", labels={"x": "y", "a": "b"}
    )
    store.wait_for_record(dst, "entry", 1000, labels={"x": "y", "a": "b"})
    store.wait_for_pending_records(replication_name)

    store.update_labels(src, "entry", 1000, {"x": "z", "1": "2"})

    resp = store.wait_for_record(dst, "entry", 1000, labels={"x": "z", "1": "2"})
    assert labels_of(resp) == {"x": "z", "a": "b", "1": "2"}
    assert resp.content == b"some_data"
    assert resp.headers["content-type"] == "text/plain"
    store.wait_for_pending_records(replication_name)


def test__local_replication_creates_missing_dst_bucket(
    base_url, session, store, bucket_name, replication_name
):
    """Should create the destination bucket if it does not exist"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.track_bucket(f"{bucket_name}_dst")  # the replication creates it
    assert session.head(f"{base_url}/b/{dst}").status_code == 404

    assert store.create_replication(replication_name, src, dst).status_code == 200
    store.write(src, "entry", 1000, b"payload")

    resp = store.wait_for_record(dst, "entry", 1000)
    assert resp.content == b"payload"
    assert session.head(f"{base_url}/b/{dst}").status_code == 200


def test__local_replication_filters_by_entries(store, bucket_name, replication_name):
    """Should replicate only the entries selected by the replication"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    resp = store.create_replication(
        replication_name, src, dst, entries=["camera/*", "!camera/rear"]
    )
    assert resp.status_code == 200

    # the selected record goes last: when it arrives, the others had their chance
    store.write(src, "lidar", 1000, b"lidar")
    store.write(src, "camera/rear", 1000, b"rear")
    store.write(src, "camera/front", 1000, b"front")

    assert store.wait_for_record(dst, "camera/front", 1000).content == b"front"
    store.wait_for_pending_records(replication_name)
    time.sleep(SETTLE_TIME)

    assert store.read(dst, "camera/rear", 1000).status_code == 404
    assert store.read(dst, "lidar", 1000).status_code == 404


def test__local_replication_filters_by_when(store, bucket_name, replication_name):
    """Should replicate only the records which match the when condition"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    resp = store.create_replication(
        replication_name, src, dst, when={"$eq": ["&quality", "high"]}
    )
    assert resp.status_code == 200

    for ts, quality in ((1000, "low"), (2000, "high"), (3000, "low"), (4000, "high")):
        store.write(
            src, "entry", ts, f"frame-{ts}".encode(), labels={"quality": quality}
        )

    for ts in (2000, 4000):
        resp = store.wait_for_record(dst, "entry", ts)
        assert resp.content == f"frame-{ts}".encode()
    store.wait_for_pending_records(replication_name)
    time.sleep(SETTLE_TIME)

    assert store.read(dst, "entry", 1000).status_code == 404
    assert store.read(dst, "entry", 3000).status_code == 404


def test__chained_local_replications(store, bucket_name, replication_name):
    """Should deliver records to the end of a chain of local replications"""
    a = store.create_bucket(f"{bucket_name}_a")
    b = store.create_bucket(f"{bucket_name}_b")
    c = store.create_bucket(f"{bucket_name}_c")
    assert store.create_replication(f"{replication_name}_ab", a, b).status_code == 200
    assert store.create_replication(f"{replication_name}_bc", b, c).status_code == 200

    store.write(
        a,
        "entry",
        1000,
        b"chained",
        "text/plain",
        labels={"site": "lab", "hops": "2"},
    )
    expected = store.read(a, "entry", 1000)

    assert_same_record(store.wait_for_record(b, "entry", 1000), expected)
    assert_same_record(store.wait_for_record(c, "entry", 1000), expected)
    store.wait_for_pending_records(f"{replication_name}_ab")
    store.wait_for_pending_records(f"{replication_name}_bc")


def test__local_replication_of_many_records(store, bucket_name, replication_name):
    """Should replicate more records than one batch can carry"""
    count = 120
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    assert store.create_replication(replication_name, src, dst).status_code == 200

    for i in range(count):
        store.write(
            src, "entry", 1000 + i, f"record-{i}".encode(), labels={"index": str(i)}
        )

    store.wait_for_record_count(dst, "entry", count)
    store.wait_for_pending_records(replication_name)
    for i in range(count):
        resp = store.read(dst, "entry", 1000 + i)
        assert resp.status_code == 200
        assert resp.content == f"record-{i}".encode()
        assert labels_of(resp) == {"index": str(i)}


def test__paused_local_replication_keeps_records(store, bucket_name, replication_name):
    """Should keep records of a paused local replication until it is enabled"""
    src = store.create_bucket(f"{bucket_name}_src")
    dst = store.create_bucket(f"{bucket_name}_dst")
    assert store.create_replication(replication_name, src, dst).status_code == 200
    store.set_mode(replication_name, "paused")

    store.write(src, "entry", 1000, b"paused")
    store.wait_for_pending_records(replication_name, 1)
    time.sleep(SETTLE_TIME)
    assert store.read(dst, "entry", 1000).status_code == 404

    store.set_mode(replication_name, "enabled")
    assert store.wait_for_record(dst, "entry", 1000).content == b"paused"
    store.wait_for_pending_records(replication_name)
