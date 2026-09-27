"""Focused tests for gRPC-backed browser update delivery."""

import json
import os
import queue
import tempfile
import time
import unittest
import urllib.request
from concurrent import futures

import chat_pb2
import chat_pb2_grpc
import grpc

from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app import grpc_server
from app.web_gateway import run_web_gateway
from storage.database import Database


class FakeContext:
    def __init__(self):
        self.active = True

    def is_active(self):
        return self.active

    def abort(self, code, details):
        raise AssertionError(f"Unexpected RPC abort: {code} {details}")


class RealtimeEventTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.temp_dir.name, "events.db"))
        self.auth = AuthManager(self.db)
        self.chat = ChatManager(self.db)
        self.servicer = grpc_server.ChatServicer.__new__(grpc_server.ChatServicer)
        self.servicer.db = self.db
        self.servicer.auth = self.auth
        self.servicer.chat = self.chat

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def create_user(self, username):
        ok, token, user_id, _, message = self.auth.signup(username, "pass1234")
        self.assertTrue(ok, message)
        return token, user_id

    def test_stream_routes_messages_and_unsubscribes_when_closed(self):
        token_a, user_a = self.create_user("stream_alice")
        token_b, user_b = self.create_user("stream_bob")
        context = FakeContext()
        stream = self.servicer.StreamMessages(
            chat_pb2.StreamMessagesRequest(token=token_b), context
        )

        self.assertEqual(next(stream).event_type, "READY")
        self.assertEqual(self.chat.subscription_count(user_b), 1)
        ok, message, error = self.chat.send_dm(user_a, user_b, "Live event")
        self.assertTrue(ok, error)

        delivered = next(stream)
        self.assertEqual(delivered.event_type, "NEW_MESSAGE")
        self.assertEqual(delivered.message.message_id, message["message_id"])
        self.assertEqual(delivered.message.content, "Live event")

        stream.close()
        self.assertEqual(self.chat.subscription_count(user_b), 0)

    def test_expired_session_emits_auth_expired_and_cleans_subscription(self):
        token, user_id = self.create_user("stream_expiry")
        context = FakeContext()
        original_interval = grpc_server.STREAM_AUTH_CHECK_INTERVAL_SECONDS
        grpc_server.STREAM_AUTH_CHECK_INTERVAL_SECONDS = 0.01
        try:
            stream = self.servicer.StreamMessages(chat_pb2.StreamMessagesRequest(token=token), context)
            self.assertEqual(next(stream).event_type, "READY")
            self.db.execute(
                "UPDATE sessions SET expires_at = ? WHERE token = ?",
                (int(time.time()) - 1, token),
            )
            self.db.commit()
            self.assertEqual(next(stream).event_type, "AUTH_EXPIRED")
            with self.assertRaises(StopIteration):
                next(stream)
            self.assertEqual(self.chat.subscription_count(user_id), 0)
        finally:
            grpc_server.STREAM_AUTH_CHECK_INTERVAL_SECONDS = original_interval

    def test_slow_subscriber_queue_overflow_requests_snapshot_resync(self):
        token, user_id = self.create_user("stream_overflow")
        subscriber = queue.Queue(maxsize=2)
        self.chat.subscribe(user_id, subscriber)
        self.chat.publish_event(user_id, "DIRECTORY_CHANGED")
        self.chat.publish_event(user_id, "DIRECTORY_CHANGED")
        self.chat.publish_event(user_id, "DIRECTORY_CHANGED")

        self.assertEqual(subscriber.qsize(), 1)
        self.assertEqual(subscriber.get_nowait()["event_type"], "RESYNC_REQUIRED")
        self.chat.unsubscribe(user_id, subscriber)

    def test_latest_hundred_messages_have_stable_monotonic_order(self):
        _, user_a = self.create_user("history_alice")
        _, user_b = self.create_user("history_bob")
        for index in range(105):
            ok, _, error = self.chat.send_dm(user_a, user_b, f"message-{index}")
            self.assertTrue(ok, error)

        history = self.chat.get_dm_history(user_a, user_b, limit=100)
        self.assertEqual(len(history), 100)
        self.assertEqual(history[0]["content"], "message-5")
        self.assertEqual(history[-1]["content"], "message-104")
        timestamps = [message["timestamp"] for message in history]
        self.assertEqual(timestamps, sorted(timestamps))
        self.assertEqual(len(timestamps), len(set(timestamps)))

    def test_revoked_group_member_does_not_receive_queued_group_content(self):
        token_admin, admin_id = self.create_user("stream_admin")
        token_member, member_id = self.create_user("stream_member")
        ok, group, error = self.chat.create_group("Updates", admin_id, [member_id])
        self.assertTrue(ok, error)

        context = FakeContext()
        stream = self.servicer.StreamMessages(
            chat_pb2.StreamMessagesRequest(token=token_member), context
        )
        self.assertEqual(next(stream).event_type, "READY")
        sent, _, error = self.chat.send_group_message(member_id, group["group_id"], "private content")
        self.assertTrue(sent, error)
        removed, _, error = self.chat.update_group(
            group_id=group["group_id"],
            requesting_user_id=admin_id,
            action="REMOVE_MEMBER",
            target_user_id=member_id,
        )
        self.assertTrue(removed, error)

        first_event = next(stream)
        self.assertEqual(first_event.event_type, "DIRECTORY_CHANGED")
        revoked_event = next(stream)
        self.assertEqual(revoked_event.event_type, "GROUP_ACCESS_REVOKED")
        self.assertEqual(revoked_event.message.group_id, group["group_id"])

        # The queued message was discarded by the membership check before either
        # control event was delivered.
        self.assertTrue(self.chat.is_group_member(group["group_id"], admin_id))
        stream.close()

    def test_logging_out_one_session_preserves_the_other(self):
        token_one, user_id = self.create_user("multi_session")
        ok, token_two, login_id, _, message = self.auth.login("multi_session", "pass1234")
        self.assertTrue(ok, message)
        self.assertEqual(login_id, user_id)

        self.assertTrue(self.auth.logout(token_one))
        self.assertFalse(self.auth.token_is_valid(token_one))
        self.assertTrue(self.auth.token_is_valid(token_two))
        self.assertEqual(self.auth.get_user_by_id(user_id)["status"], "active")

        self.assertTrue(self.auth.logout(token_two))
        self.assertEqual(self.auth.get_user_by_id(user_id)["status"], "inactive")

    def test_activity_reactivates_presence_and_invalidates_directories(self):
        token, user_id = self.create_user("presence_return")
        subscriber = queue.Queue(maxsize=4)
        self.chat.subscribe(user_id, subscriber)
        self.db.execute(
            "UPDATE users SET status = 'inactive', last_seen = ? WHERE user_id = ?",
            (int(time.time()), user_id),
        )
        self.db.commit()

        session = self.servicer._require_auth(token, FakeContext())
        self.assertEqual(session["status"], "inactive")
        self.assertEqual(self.auth.get_user_by_id(user_id)["status"], "active")
        self.assertEqual(subscriber.get_nowait()["event_type"], "DIRECTORY_CHANGED")
        self.chat.unsubscribe(user_id, subscriber)

    def test_http_sse_bridge_and_32_streams_leave_unary_listener_responsive(self):
        unary_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        stream_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=32), maximum_concurrent_rpcs=32
        )
        chat_pb2_grpc.add_ChatServiceServicer_to_server(self.servicer, unary_server)
        chat_pb2_grpc.add_ChatServiceServicer_to_server(self.servicer, stream_server)
        unary_port = unary_server.add_insecure_port("127.0.0.1:0")
        stream_port = stream_server.add_insecure_port("127.0.0.1:0")
        unary_server.start()
        stream_server.start()

        unary_channel = grpc.insecure_channel(f"127.0.0.1:{unary_port}")
        stream_channel = grpc.insecure_channel(f"127.0.0.1:{stream_port}")
        unary_stub = chat_pb2_grpc.ChatServiceStub(unary_channel)
        stream_stub = chat_pb2_grpc.ChatServiceStub(stream_channel)
        httpd = None
        response = None
        streams = []
        try:
            alice = unary_stub.Signup(chat_pb2.SignupRequest(username="sse_alice", password="pass1234"))
            bob = unary_stub.Signup(chat_pb2.SignupRequest(username="sse_bob", password="pass1234"))
            self.assertTrue(alice.success)
            self.assertTrue(bob.success)

            httpd = run_web_gateway(
                port=0,
                grpc_target=f"127.0.0.1:{unary_port}",
                stream_grpc_target=f"127.0.0.1:{stream_port}",
            )
            events_url = (
                f"http://127.0.0.1:{httpd.server_address[1]}/api/events"
                f"?token={bob.token}"
            )
            response = urllib.request.urlopen(events_url, timeout=5)

            def read_sse_event():
                event_type = None
                data_lines = []
                while True:
                    line = response.readline().decode("utf-8").rstrip("\r\n")
                    if not line:
                        if event_type is not None:
                            return event_type, json.loads("\n".join(data_lines))
                        continue
                    field, separator, value = line.partition(":")
                    value = value[1:] if separator and value.startswith(" ") else value
                    if field == "event":
                        event_type = value
                    elif field == "data":
                        data_lines.append(value)

            event_type, _ = read_sse_event()
            self.assertEqual(event_type, "READY")
            sent = unary_stub.SendDirectMessage(
                chat_pb2.SendDirectMessageRequest(
                    token=alice.token,
                    recipient_user_id=bob.user_id,
                    content="forwarded over SSE",
                    client_request_id="sse-bridge-message",
                )
            )
            self.assertTrue(sent.success)
            event_type, payload = read_sse_event()
            self.assertEqual(event_type, "NEW_MESSAGE")
            self.assertEqual(payload["message"]["content"], "forwarded over SSE")

            response.close()
            response = None
            deadline = time.time() + 8
            while self.chat.subscription_count(bob.user_id) and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(self.chat.subscription_count(bob.user_id), 0)

            streams = [
                stream_stub.StreamMessages(chat_pb2.StreamMessagesRequest(token=bob.token))
                for _ in range(32)
            ]
            for stream in streams:
                self.assertEqual(next(stream).event_type, "READY")

            started = time.monotonic()
            users = unary_stub.ListUsers(chat_pb2.ListUsersRequest(token=alice.token), timeout=2)
            self.assertGreaterEqual(len(users.users), 1)
            self.assertLess(time.monotonic() - started, 2)
        finally:
            if response is not None:
                response.close()
            for stream in streams:
                stream.cancel()
            if httpd is not None:
                httpd.shutdown()
                httpd.server_close()
                for channel in httpd.grpc_channels:
                    channel.close()
            unary_channel.close()
            stream_channel.close()
            unary_server.stop(grace=0)
            stream_server.stop(grace=0)

if __name__ == "__main__":
    unittest.main()
