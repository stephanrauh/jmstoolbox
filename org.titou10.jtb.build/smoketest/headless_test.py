#!/usr/bin/env python3
# Tests a running JMSToolBox through its REST services (started by headless.sh).
# For each session of config.xml: list the destinations, empty the test queue, send a message, browse it, empty the queue.
# Every request has a timeout, so a hanging Q Manager plugin fails the test instead of blocking it.
# Only uses the Python standard library.
import json
import base64
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BASE_URL = "http://localhost:9998/rest"
TIMEOUT = 60  # seconds per request

# Test queue of each session, created by setup.sh: a name of its own per broker, so that a test can not see the queue of
# another broker. Oracle AQ doesn't allow dots in queue names; JMSToolBox shows its queues with the owner: JTB.JTB_TEST_ORACLE
TEST_QUEUES = {
    "ActiveMQ Classic": "JTB.TEST.ACTIVEMQ",
    "Artemis 2.x": "JTB.TEST.ARTEMIS2",
    "Artemis 1.x": "JTB.TEST.ARTEMIS1",
    "Oracle AQ": "JTB_TEST_ORACLE",
    "Solace": "JTB.TEST.SOLACE",
}

# Number of queues and of topics created by setup.sh in ActiveMQ Classic for the discovery tests
LOAD_COUNT = 600


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            text = resp.read().decode()
            return resp.status, (json.loads(text) if text else None)
    except urllib.error.HTTPError as e:
        raise AssertionError(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:300]}") from None
    except TimeoutError:
        raise AssertionError(f"{method} {path} -> no answer within {TIMEOUT} s") from None
    except urllib.error.URLError as e:
        raise AssertionError(f"{method} {path} -> {e.reason}") from None


def q(name):
    return urllib.parse.quote(name, safe="")


def test_session(session):
    s = q(session)

    status, destinations = request("GET", f"/session/{s}")
    names = [d["name"] for d in destinations or []]
    test_queue = TEST_QUEUES[session]
    queue = next((n for n in names if n == test_queue or n.endswith("." + test_queue)), None)
    if queue is None:
        raise AssertionError(f"test queue {test_queue} not found among {len(names)} destinations: {names[:10]}")
    d = q(queue)

    request("DELETE", f"/message/{s}/{d}")

    payload = f"jtb-smoketest-{session}-{time.time_ns()}"
    status, sent = request("POST", f"/message/{s}/{d}", {"type": "TEXT", "payloadText": payload})
    if status != 201:
        raise AssertionError(f"sending returned HTTP {status}")

    # Some brokers make a message visible to browsers only after a short delay
    for _ in range(10):
        status, browsed = request("GET", f"/message/{s}/{d}")
        if any(m.get("payloadText") == payload for m in browsed or []):
            break
        time.sleep(1)
    else:
        raise AssertionError(f"sent message not found when browsing {queue}")

    request("DELETE", f"/message/{s}/{d}")
    status, browsed = request("GET", f"/message/{s}/{d}")
    if browsed:
        raise AssertionError(f"{queue} still contains {len(browsed)} message(s) after emptying it")

    return f"{len(names)} destinations, send/browse/empty OK on {queue}"


def test_discovery(session):
    # setup.sh creates JTB.LOAD.Q.000-599 and JTB.LOAD.T.000-599. All of them must be listed on the first listing
    status, destinations = request("GET", f"/session/{q(session)}")
    names = {d["name"] for d in destinations or []}
    expected = {f"JTB.LOAD.{kind}.{i:03d}" for kind in ("Q", "T") for i in range(LOAD_COUNT)}
    missing = expected - names
    queues = sum(1 for n in names if n.startswith("JTB.LOAD.Q."))
    topics = sum(1 for n in names if n.startswith("JTB.LOAD.T."))
    if missing:
        raise AssertionError(f"listed {queues} of {LOAD_COUNT} queues and {topics} of {LOAD_COUNT} topics "
                             f"({len(missing)} missing, e.g. {sorted(missing)[:3]})")
    return f"all {LOAD_COUNT} queues and {LOAD_COUNT} topics listed"


def jolokia(operation, name):
    body = {"type": "exec", "mbean": "org.apache.activemq:type=Broker,brokerName=localhost",
            "operation": operation, "arguments": [name]}
    req = urllib.request.Request("http://localhost:8162/api/jolokia/", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "Origin": "http://localhost",
                                          "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        result = json.load(response)
        if result.get("status") != 200:
            raise AssertionError(f"Jolokia {operation} failed: {result}")


def test_live_discovery():
    session = "Discovery 1"
    path = f"/session/{q(session)}?discoveryTimeoutMs=0"
    request("GET", path)  # Connect first: the following destinations are genuinely new.
    name = f"JTB.LIVE.{time.time_ns()}"
    expected = {(name, "QUEUE"), (name, "TOPIC")}

    def wait_for(present):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            _, destinations = request("GET", path)
            found = {(d["name"], d["type"]) for d in destinations or []}
            matches = expected <= found if present else not (expected & found)
            if matches:
                return
            time.sleep(0.1)
        raise AssertionError(f"live destinations did not {'appear' if present else 'disappear'}")

    try:
        jolokia("addQueue", name)
        jolokia("addTopic", name)
        wait_for(True)
    finally:
        jolokia("removeQueue", name)
        jolokia("removeTopic", name)
    wait_for(False)
    return "queue and topic additions/removals visible on the existing connection"


def test_discovery_parameters():
    path = f"/session/{q('Discovery 1')}"
    for query in ("discoveryTimeoutMs=-1", "discoveryTimeoutMs=60001", "discoveryQuietPeriodMs=-1",
                  "discoveryQuietPeriodMs=60001", "discoveryTimeoutMs=abc", "discoveryQuietPeriodMs=1.5",
                  "discoveryTimeoutMs=99999999999999999999999"):
        try:
            with urllib.request.urlopen(BASE_URL + path + "?" + query, timeout=TIMEOUT):
                pass
        except urllib.error.HTTPError as e:
            if e.code != 400:
                raise AssertionError(f"{query}: expected HTTP 400, got {e.code}") from None
        else:
            raise AssertionError(f"{query}: invalid value was accepted")

    # Either zero disables waiting, even if the other parameter requests a long wait.
    for query in ("discoveryTimeoutMs=0&discoveryQuietPeriodMs=60000",
                  "discoveryTimeoutMs=60000&discoveryQuietPeriodMs=0"):
        start = time.monotonic()
        request("GET", path + "?" + query)
        if time.monotonic() - start > 3:
            raise AssertionError(f"{query}: zero must disable discovery waiting")
    return "parameter validation and immediate snapshots OK"


def main():
    config = sys.argv[1] if len(sys.argv) > 1 else "config.xml"
    sessions = [s.get("name") for s in ET.parse(config).getroot().iter("sessionDef")]

    failures = 0
    checks = [(session, lambda session=session: (test_discovery if session.startswith("Discovery") else test_session)(session))
              for session in sessions]
    checks += [("Live discovery", test_live_discovery), ("Discovery parameters", test_discovery_parameters)]
    for label, test in checks:
        start = time.time()
        try:
            result, ok = test(), True
        except AssertionError as e:
            result, ok = str(e), False
            failures += 1
        print(f"{'PASS' if ok else 'FAIL'}  {label:<20} {time.time() - start:5.1f} s  {result}", flush=True)

    print(f"\n{len(checks) - failures} of {len(checks)} checks passed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
