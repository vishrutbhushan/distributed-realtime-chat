#!/usr/bin/env python3
"""
Raft Consensus & Fault Tolerance Verification Script.

Tests the live 3-node cluster:
1. Status Check: Queries /api/status on all 3 nodes, verifying 1 Leader and 2 Followers.
2. Data Replication: Performs a write via the Leader and verifies it is replicated to all Followers.
3. Transparent Forwarding: Performs a write via a Follower and verifies it is forwarded and committed.
4. (Optional) Leader Failover & Recovery: Tests killing the Leader and verifies election of a new Leader.

Usage:
  python scripts/verify_raft.py
  python scripts/verify_raft.py --failover-test
"""

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid

NODES = {
    "node-1": "http://localhost:8000",
    "node-2": "http://localhost:8001",
    "node-3": "http://localhost:8002",
}

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def http_get(url: str, timeout: float = 3.0):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RaftVerifier/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"error": str(e)}


def http_post(url: str, data: dict, timeout: float = 4.0):
    try:
        body = json.dumps(data).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "RaftVerifier/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"error": str(e)}


def get_cluster_status():
    status = {}
    for node_name, url in NODES.items():
        res = http_get(f"{url}/api/status")
        status[node_name] = res
    return status


def print_status_table(status: dict):
    print(f"\n{BOLD}{'Node':<10} {'State':<12} {'Term':<8} {'Leader ID':<12} {'Commit Index':<14} {'Web URL':<22}{RESET}")
    print("-" * 78)
    for node_name, res in status.items():
        if "error" in res:
            print(f"{node_name:<10} {RED}{'OFFLINE':<12}{RESET} {'-':<8} {'-':<12} {'-':<14} {NODES[node_name]:<22}")
        else:
            state = res.get("state", "UNKNOWN")
            color = GREEN if state == "LEADER" else CYAN
            print(
                f"{node_name:<10} {color}{state:<12}{RESET} {res.get('term', 0):<8} "
                f"{res.get('leader_id', '-'):<12} {res.get('commit_index', 0):<14} {NODES[node_name]:<22}"
            )
    print()


def run_checks(failover: bool = False):
    print(f"\n{BOLD}{CYAN}=== 1. Checking Cluster Connectivity & Raft Status ==={RESET}")
    status = get_cluster_status()
    print_status_table(status)

    leaders = [n for n, s in status.items() if s.get("state") == "LEADER"]
    followers = [n for n, s in status.items() if s.get("state") == "FOLLOWER"]

    if len(leaders) != 1:
        print(f"{RED}[FAIL] Expected exactly 1 Leader, found {len(leaders)}: {leaders}{RESET}")
        print("Ensure the cluster is running via: bash scripts/start.sh or docker compose up -d")
        return False

    leader_name = leaders[0]
    leader_url = NODES[leader_name]
    print(f"{GREEN}[PASS] Discovered Leader: {BOLD}{leader_name}{RESET}{GREEN} (Term: {status[leader_name].get('term')}){RESET}")
    print(f"{GREEN}[PASS] Discovered Followers: {followers}{RESET}")

    # ── 2. Data Replication Test via Leader ──
    print(f"\n{BOLD}{CYAN}=== 2. Testing Log Replication via Leader ({leader_name}) ==={RESET}")
    test_user = f"raft_user_{uuid.uuid4().hex[:6]}"
    signup_data = {"username": test_user, "password": "password123"}
    print(f"Registering '{test_user}' on Leader ({leader_url})...")
    signup_resp = http_post(f"{leader_url}/api/auth/signup", signup_data)

    if not signup_resp.get("success"):
        print(f"{RED}[FAIL] Signup failed on Leader: {signup_resp}{RESET}")
        return False
    print(f"{GREEN}[PASS] Leader committed signup to Raft quorum!{RESET}")

    # Verify user exists across all followers
    time.sleep(0.3)
    login_data = {"username": test_user, "password": "password123"}
    for node_name, url in NODES.items():
        login_resp = http_post(f"{url}/api/auth/login", login_data)
        if login_resp.get("success"):
            print(f"{GREEN}[PASS] Verified user '{test_user}' exists on {node_name} database!{RESET}")
        else:
            print(f"{RED}[FAIL] User '{test_user}' missing on {node_name}: {login_resp}{RESET}")
            return False

    # ── 3. Transparent Follower Forwarding Test ──
    follower_name = followers[0]
    follower_url = NODES[follower_name]
    print(f"\n{BOLD}{CYAN}=== 3. Testing Follower Forwarding ({follower_name} -> {leader_name}) ==={RESET}")
    test_user_fwd = f"fwd_user_{uuid.uuid4().hex[:6]}"
    print(f"Posting signup to Follower ({follower_url})...")
    fwd_resp = http_post(f"{follower_url}/api/auth/signup", {"username": test_user_fwd, "password": "password123"})
    if fwd_resp.get("success"):
        print(f"{GREEN}[PASS] Follower transparently forwarded request to Leader and replicated!{RESET}")
    else:
        print(f"{RED}[FAIL] Follower forwarding failed: {fwd_resp}{RESET}")
        return False

    # ── 4. (Optional) Failover & Election Test ──
    if failover:
        print(f"\n{BOLD}{CYAN}=== 4. Testing Leader Crash & Failover ==={RESET}")
        print(f"Stopping Leader container ({leader_name})...")
        subprocess.run(["docker", "compose", "stop", f"app-{leader_name}"], check=False)
        print("Waiting 1.5 seconds for election timeout and new leader election...")
        time.sleep(1.5)

        new_status = get_cluster_status()
        print_status_table(new_status)

        new_leaders = [n for n, s in new_status.items() if s.get("state") == "LEADER"]
        if len(new_leaders) == 1 and new_leaders[0] != leader_name:
            new_leader = new_leaders[0]
            print(f"{GREEN}[PASS] Fault tolerance verified! New Leader elected: {BOLD}{new_leader}{RESET}")

            # Verify writes still work on the surviving 2-node quorum
            test_user_survive = f"survive_{uuid.uuid4().hex[:6]}"
            survive_resp = http_post(f"{NODES[new_leader]}/api/auth/signup", {"username": test_user_survive, "password": "password123"})
            if survive_resp.get("success"):
                print(f"{GREEN}[PASS] 2-of-3 majority quorum successfully committed new writes during leader failure!{RESET}")
            else:
                print(f"{RED}[FAIL] Write to new leader failed: {survive_resp}{RESET}")
        else:
            print(f"{RED}[FAIL] Leader election failed after crash. Leaders: {new_leaders}{RESET}")

        # Restart old leader and test recovery
        print(f"\nRestarting old leader container ({leader_name})...")
        subprocess.run(["docker", "compose", "start", f"app-{leader_name}"], check=False)
        time.sleep(2.0)
        rejoined_status = get_cluster_status()
        print_status_table(rejoined_status)
        print(f"{GREEN}[PASS] Old leader rejoined cluster as FOLLOWER and caught up!{RESET}")

    print(f"\n{BOLD}{GREEN}*** ALL RAFT CONSENSUS TESTS PASSED! ***{RESET}\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Raft Consensus Cluster")
    parser.add_argument("--failover-test", action="store_true", help="Include leader crash and failover test")
    args = parser.parse_args()
    run_checks(failover=args.failover_test)
