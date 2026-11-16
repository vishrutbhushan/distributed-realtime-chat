"""Read live node telemetry and run demos against this Compose project."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import socket
from threading import Lock, Thread
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4


PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
NUMBER_LABEL = "com.docker.compose.container-number"
NODE_LIMIT = 12
STATIC = Path(__file__).parent / "static"


class DockerConnection(HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def engine(method, path, data=None):
    conn = DockerConnection("localhost", timeout=8)
    try:
        headers = {"Content-Type": "application/json"} if data is not None else {}
        conn.request(method, path, json.dumps(data) if data is not None else None, headers)
        response = conn.getresponse()
        payload = response.read()
        parsed = json.loads(payload) if payload else {}
        if response.status >= 300:
            raise RuntimeError(parsed.get("message", f"Docker returned {response.status}"))
        return parsed
    finally:
        conn.close()


def app_request(container, path, data=None, timeout=2):
    req = Request(
        f"http://{container}:8000{path}", data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


class Monitor:
    def __init__(self, project):
        self.project = project
        self.prefix = f"{project}-app-node-"
        self.lock = Lock()
        self.action_lock = Lock()
        self.nodes = []
        self.events = deque(maxlen=1600)
        self.election_events = deque(maxlen=120)
        self.cursors = {}
        self.error = None
        self.sequence = 0
        self.demo = None
        self.accounts = None
        self.last_lifecycle = {}

    def containers(self):
        query = urlencode({
            "all": "true",
            "filters": json.dumps({"label": [f"{PROJECT_LABEL}={self.project}", f"{SERVICE_LABEL}=app-node"]}),
        })
        result = {}
        for row in engine("GET", "/containers/json?" + query):
            slot = row["Labels"].get(NUMBER_LABEL, "")
            if slot.isdecimal() and "/" + self.prefix + slot in row["Names"]:
                result[f"node-{slot}"] = row
        return result

    def _event(self, event):
        self.sequence += 1
        observed = {"id": self.sequence, **event}
        self.events.append(observed)
        if event["kind"] in ("election_started", "vote_cast", "vote_reply", "leader_elected"):
            self.election_events.append(observed)

    def record(self, kind, **fields):
        with self.lock:
            self._event({"time": time.time(), "kind": kind, **fields})

    def _read_node(self, node, row):
        state = row["State"]
        item = {"node_id": node, "docker_state": state, "online": False}
        if state != "running":
            return item, [], None
        boot, after = self.cursors.get(node, ("", 0))
        try:
            telemetry = app_request(
                row["Names"][0].lstrip("/"),
                "/internal/raft-monitor?" + urlencode({"boot": boot, "after": after}),
                timeout=0.8,
            )
        except (OSError, ValueError):
            return item, [], None
        if telemetry.get("node_id") != node:
            return item, [], None
        boot_changed = telemetry["boot"] != boot
        observed = telemetry.pop("events")
        if telemetry.pop("gap"):
            observed.insert(0, {"kind": "telemetry_gap", "time": time.time()})
        cursor = max((e.get("seq", 0) for e in observed), default=0 if boot_changed else after)
        return {**item, **telemetry, "online": True}, observed, (telemetry["boot"], cursor)

    def collect(self):
        with ThreadPoolExecutor(max_workers=NODE_LIMIT) as pool:
            while True:
                try:
                    containers = self.containers()
                    jobs = {node: pool.submit(self._read_node, node, row) for node, row in containers.items()}
                    samples = {node: job.result() for node, job in jobs.items()}
                    with self.lock:
                        new_events = []
                        self.nodes = []
                        for node in sorted(samples, key=lambda x: int(x.split("-")[1])):
                            item, events, cursor = samples[node]
                            self.nodes.append(item)
                            if cursor is not None:
                                self.cursors[node] = cursor
                            previous = self.last_lifecycle.get(node)
                            lifecycle = item["docker_state"], item["online"]
                            if previous != lifecycle:
                                self._event({
                                    "kind": "node_online" if item["online"] else "node_unreachable",
                                    "node": node, "time": time.time(), "docker_state": item["docker_state"],
                                })
                            self.last_lifecycle[node] = lifecycle
                            new_events.extend({"node": node, **event} for event in events)
                        for node in self.last_lifecycle.keys() - samples.keys():
                            self._event({"kind": "node_removed", "node": node, "time": time.time()})
                            self.last_lifecycle.pop(node)
                            self.cursors.pop(node, None)
                        for event in sorted(new_events, key=lambda e: e["time"]):
                            self._event(event)
                        self.error = None
                except (OSError, RuntimeError, ValueError, KeyError) as exc:
                    with self.lock:
                        self.error = str(exc)
                time.sleep(0.35)

    def snapshot(self, after):
        with self.lock:
            events = [event for event in self.events if event["id"] > after]
            return {
                "project": self.project, "nodes": self.nodes, "events": events,
                "cursor": self.sequence, "error": self.error, "demo": self.demo,
                "elections": list(self.election_events),
                "busy": self.action_lock.locked(),
            }

    def _leader(self, containers):
        with self.lock:
            leaders = [
                item["node_id"] for item in self.nodes
                if item.get("state") == "LEADER" and item["online"]
                and containers.get(item["node_id"], {}).get("State") == "running"
            ]
        if len(leaders) != 1:
            raise RuntimeError("Wait for one reachable leader before running this action.")
        return leaders[0]

    def _recovery_target(self):
        with self.lock:
            leaders = [item for item in self.nodes if item["online"] and item.get("state") == "LEADER"]
            return leaders[0]["commit_index"] if len(leaders) == 1 else None

    def _send(self, containers):
        leader = self._leader(containers)
        leader_name = self.prefix + leader.split("-")[1]
        if self.accounts is None:
            suffix = uuid4().hex[:9]
            accounts = []
            for name in ("monitor_a_", "monitor_b_"):
                response = app_request(leader_name, "/api/signup", {
                    "username": name + suffix, "password": uuid4().hex,
                }, timeout=6)
                if not response.get("success"):
                    raise RuntimeError(response.get("message") or "Could not create the demo accounts.")
                accounts.append(response)
            self.accounts = accounts
        sender, recipient = self.accounts
        with self.lock:
            followers = [
                item["node_id"] for item in self.nodes if item["node_id"] != leader
                and item["online"] and item.get("leader_id") == leader
                and containers.get(item["node_id"], {}).get("State") == "running"
            ]
        node = followers[0] if followers else leader
        request_id = uuid4().hex
        message = f"Raft demo {request_id[:7]}"
        with self.lock:
            self.demo = {"request_id": request_id, "text": message, "node": node, "outcome": "pending"}
        self.record("demo_submitted", node=node, request_id=request_id)
        try:
            response = app_request(self.prefix + node.split("-")[1], "/api/messages/dm", {
                "token": sender["token"], "recipient_user_id": recipient["user_id"],
                "content": message, "client_request_id": request_id,
            }, timeout=10)
            if not response.get("success"):
                raise RuntimeError(response.get("error") or "Chat rejected the write.")
        except (OSError, RuntimeError, ValueError):
            with self.lock:
                self.demo = {**self.demo, "outcome": "unconfirmed"}
            self.record("demo_unconfirmed", node=node, request_id=request_id)
            raise
        confirmed = response["message"]
        with self.lock:
            self.demo = {**self.demo, "message_id": confirmed["message_id"], "outcome": "confirmed"}
        self.record("demo_confirmed", node=node, request_id=request_id)
        return f"Chat confirmed {message}."

    def _add(self, containers):
        if not containers:
            raise RuntimeError("Start Compose with at least one app replica first.")
        if len(containers) >= NODE_LIMIT:
            raise RuntimeError(f"The dashboard supports up to {NODE_LIMIT} app replicas.")
        suffix = max(int(node.split("-")[1]) for node in containers) + 1
        if suffix > 64:
            raise RuntimeError("No supported Compose replica slots remain.")
        template = next((row for row in containers.values() if row["State"] == "running"), None)
        if not template:
            raise RuntimeError("Restart an app replica before adding a new one.")
        source = engine("GET", f"/containers/{template['Id']}/json")
        name = self.prefix + str(suffix)
        config = source["Config"]
        labels = {key: val for key, val in config["Labels"].items() if key.startswith("com.docker.compose.")}
        labels[NUMBER_LABEL] = str(suffix)
        host = source["HostConfig"]
        network = f"{self.project}_chat-net"
        created = engine("POST", "/containers/create?" + urlencode({"name": name}), {
            "Image": source["Image"],
            "Env": config["Env"], "Cmd": config["Cmd"], "Entrypoint": config["Entrypoint"],
            "Labels": labels,
            "HostConfig": {**{key: host[key] for key in ("Binds", "Mounts", "RestartPolicy") if key in host},
                           "NetworkMode": network},
            "NetworkingConfig": {"EndpointsConfig": {network: {"Aliases": [name, "app-node"]}}},
        })
        target = self._recovery_target()
        started_at = time.time()
        engine("POST", f"/containers/{created['Id']}/start")
        self.record("control_add", node=f"node-{suffix}", time=started_at, target_index=target)
        return {"message": f"Started node-{suffix}. Watch it discover the leader and catch up.",
                "node": f"node-{suffix}"}

    def action(self, action, node=None):
        if not self.action_lock.acquire(blocking=False):
            raise RuntimeError("Another dashboard action is in progress.")
        try:
            containers = self.containers()
            if action == "send":
                return {"message": self._send(containers)}
            if action == "add":
                return self._add(containers)
            if action == "stop_leader":
                node = self._leader(containers)
                action = "stop"
            if action not in {"stop", "start"} or node not in containers:
                raise RuntimeError("Select a replica from this Compose project.")
            container = containers[node]
            expected = "running" if action == "stop" else "exited"
            if container["State"] != expected and not (action == "start" and container["State"] == "created"):
                raise RuntimeError(f"{node} is {container['State']}.")
            if action == "stop":
                # A zero second timeout force-stops it and suppresses Docker's auto-restart.
                engine("POST", f"/containers/{container['Id']}/stop?t=0")
                self.record("control_stop", node=node)
            else:
                target = self._recovery_target()
                started_at = time.time()
                engine("POST", f"/containers/{container['Id']}/start")
                self.record("control_start", node=node, time=started_at, target_index=target)
            return {"message": f"{'Stopped' if action == 'stop' else 'Started'} {node}.", "node": node}
        finally:
            self.action_lock.release()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def _allowed_host(self):
        try:
            return urlsplit("//" + self.headers.get("Host", "")).hostname in {"localhost", "127.0.0.1", "::1"}
        except ValueError:
            return False

    def _respond(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, value):
        self._respond(status, "application/json", json.dumps(value).encode())

    def do_GET(self):
        if not self._allowed_host():
            return self._json(403, {"error": "Open the dashboard through localhost."})
        path = urlsplit(self.path)
        if path.path == "/api/state":
            from urllib.parse import parse_qs
            try:
                after = int(parse_qs(path.query).get("after", [0])[0])
            except ValueError:
                return self._json(400, {"error": "Invalid cursor"})
            return self._json(200, self.server.monitor.snapshot(after))
        files = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"),
                 "/style.css": ("style.css", "text/css")}
        if path.path not in files:
            return self._json(404, {"error": "Route not found"})
        filename, kind = files[path.path]
        self._respond(200, kind + "; charset=utf-8", (STATIC / filename).read_bytes())

    def do_POST(self):
        if self.path != "/api/action":
            return self._json(404, {"error": "Route not found"})
        if not self._allowed_host() or self.headers.get("Origin") != "http://" + self.headers.get("Host", ""):
            return self._json(403, {"error": "Actions require the local dashboard origin."})
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 1024 or self.headers.get_content_type() != "application/json":
                raise ValueError("Expected a small JSON action.")
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict) or not isinstance(data.get("action"), str):
                raise ValueError("Invalid action")
            node = data.get("node")
            if node is not None and (not isinstance(node, str) or not re.fullmatch(r"node-[1-9][0-9]*", node)):
                raise ValueError("Invalid replica ID")
            result = self.server.monitor.action(data["action"], node)
        except (OSError, RuntimeError, ValueError) as exc:
            return self._json(409, {"error": str(exc)})
        self._json(200, result)


if __name__ == "__main__":
    monitor = Monitor(os.environ["COMPOSE_PROJECT_NAME"])
    Thread(target=monitor.collect, daemon=True).start()
    server = ThreadingHTTPServer(("", 8080), Handler)
    server.monitor = monitor
    server.serve_forever()
