#!/usr/bin/env python3
"""Verify live Raft replication and cookie affinity through the Compose load balancer."""

import argparse
from contextlib import ExitStack
from http.cookiejar import CookieJar
import json
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import build_opener, HTTPCookieProcessor, Request
from uuid import uuid4


class Client:
    def __init__(self, base):
        self.base = base
        self.opener = build_opener(HTTPCookieProcessor(CookieJar()))

    def open(self, path, query=None, data=None, timeout=9):
        url = self.base + path + ("?" + urlencode(query) if query else "")
        req = Request(url, data=json.dumps(data).encode() if data is not None else None,
                      headers={"Content-Type": "application/json"})
        return self.opener.open(req, timeout=timeout)

    def json(self, path, query=None, data=None):
        with self.open(path, query, data) as response:
            result = json.load(response)
        require(result.get("success"), f"{path} failed: {result.get('error') or result.get('message')}")
        return result

    def node(self):
        return self.json("/api/status")["node_id"]


def require(condition, error):
    if not condition:
        raise RuntimeError(error)


def wait_for(check, description, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {description}.")


def containers():
    rows = subprocess.check_output(
        ["docker", "compose", "ps", "--format", "json", "app-node"], text=True
    )
    return {f"node-{name.rsplit('-', 1)[-1]}": name for line in rows.splitlines()
            if (name := json.loads(line)["Name"])}


def node_status(container):
    # Diagnostic access stays on the Docker network. The HTTP client has no node selector.
    command = (
        "from urllib.request import urlopen; "
        "print(urlopen('http://127.0.0.1:8000/api/status', timeout=1).read().decode())"
    )
    result = subprocess.run(["docker", "exec", container, "python", "-c", command],
                            capture_output=True, text=True, timeout=4)
    return json.loads(result.stdout) if result.returncode == 0 else {}


def pick_leader(nodes):
    leaders = [node for node, container in nodes.items() if node_status(container).get("state") == "LEADER"]
    return leaders[0] if len(leaders) == 1 else None


def client_for(base, wanted):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            client = Client(base)
            node = client.node()
            if node in wanted and all(client.node() == node for _ in range(3)):
                return client, node
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"Could not find an affinity cookie for {', '.join(wanted)}.")


def expect_event(response, wanted, message_id=None):
    event = ""
    for _ in range(150):
        line = response.readline()
        if not line:
            break
        text = line.decode().rstrip("\r\n")
        if text.startswith("event: "):
            event = text[7:]
        if event == wanted and text.startswith("data: "):
            payload = json.loads(text[6:])
            if message_id is None or payload.get("message", {}).get("message_id") == message_id:
                return
    raise RuntimeError(f"SSE did not deliver {wanted}.")


def send(client, sender, recipient, text, group=False):
    target = "group_id" if group else "recipient_user_id"
    return client.json(f"/api/messages/{'group' if group else 'dm'}", data={
        "token": sender["token"], target: recipient, "content": text, "client_request_id": uuid4().hex,
    })["message"]["message_id"]


def verify(base, failover):
    nodes = containers()
    require(len(nodes) >= 3, "Start the Compose app-node service first.")
    leader = wait_for(lambda: pick_leader(nodes), "a single leader")
    leader_client, _ = client_for(base, {leader})
    follower, follower_id = client_for(base, nodes.keys() - {leader})
    print(f"PASS session cookies stick to distinct replicas ({leader}, {follower_id}).")

    suffix = uuid4().hex[:7]
    alice = follower.json("/api/signup", data={"username": f"check_a_{suffix}", "password": uuid4().hex})
    bob_password = uuid4().hex
    bob_name = f"check_b_{suffix}"
    bob = follower.json("/api/signup", data={"username": bob_name, "password": bob_password})
    bob = follower.json("/api/login", data={"username": bob_name, "password": bob_password})
    group = follower.json("/api/groups", data={
        "token": alice["token"], "name": "Verification", "initial_member_user_ids": [bob["user_id"]],
    })["group"]
    query = {"token": alice["token"]}
    require(any(g["group_id"] == group["group_id"] for g in follower.json("/api/groups", query)["groups"]),
            "The serving follower could not immediately read the new group.")
    renamed = "Renamed " + suffix
    follower.json("/api/groups/update", data={
        **query, "group_id": group["group_id"], "action": "RENAME", "new_name": renamed,
    })
    require(any(g["name"] == renamed for g in follower.json("/api/groups", query)["groups"]),
            "The serving follower could not immediately read the group rename.")

    with ExitStack() as stack:
        feeds = []
        for _ in range(4):
            feed = stack.enter_context(follower.open("/api/events", {"token": bob["token"]}))
            expect_event(feed, "READY")
            feeds.append(feed)
        require(follower.node() == follower_id, "Open SSE connections changed the session's affinity.")
        dm = send(follower, alice, bob["user_id"], "Direct verification " + suffix)
        history = follower.json("/api/messages/dm", {**query, "other_user_id": bob["user_id"]})["messages"]
        require(any(m["message_id"] == dm for m in history), "The serving follower could not immediately read the DM.")
        for feed in feeds:
            expect_event(feed, "NEW_MESSAGE", dm)
        group_id = group["group_id"]
        message = send(follower, alice, group_id, "Group verification " + suffix, group=True)
        history = follower.json("/api/messages/group", {**query, "group_id": group_id})["messages"]
        require(any(m["message_id"] == message for m in history), "The serving follower could not immediately read the group message.")
    print("PASS immediate follower reads after signup, login, group changes, and both message types.")
    print("PASS four simultaneous SSE connections on one affinity cookie received the real DM.")

    if failover:
        old = nodes[leader]
        subprocess.run(["docker", "stop", "--time", "0", old], check=True, capture_output=True)
        try:
            surviving = {node: name for node, name in nodes.items() if node != leader}
            replacement = wait_for(lambda: pick_leader(surviving), "a replacement leader")
            require(leader_client.node() != leader, "The original browser cookie did not fail over.")
            with leader_client.open("/api/events", {"token": bob["token"]}) as feed:
                expect_event(feed, "READY")
                sent = send(leader_client, alice, bob["user_id"], "Failover verification " + suffix)
                expect_event(feed, "NEW_MESSAGE", sent)
            index = node_status(surviving[replacement])["commit_index"]
            print(f"PASS {replacement} took over; the same cookie and chat session received SSE after failover.")
        finally:
            subprocess.run(["docker", "start", old], check=True, capture_output=True)
        wait_for(lambda: node_status(old).get("last_applied", -1) >= index, "the restarted leader to catch up")
        print(f"PASS {leader} restarted and applied through log entry #{index}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--failover-test", action="store_true", help="Stop and restart the current leader container")
    parser.add_argument("--url", default="http://localhost:8000", help="Nginx URL")
    args = parser.parse_args()
    try:
        verify(args.url.rstrip("/"), args.failover_test)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"FAIL {exc}\n")
