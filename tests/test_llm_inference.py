"""
LLM prompt, bounding, and response handling tests that do not require model downloads.
"""

import os
import time
import unittest
from unittest.mock import patch

import llm_pb2
from llm.server import LLMServicer
from llm.inference import ChatInference, _bound_messages, _parse_smart_replies
from llm.prompts import format_smart_reply


class FakeModel:
    def __init__(self, answer="First\nSecond\nThird"):
        self.answer = answer
        self.prompts = []

    def create_chat_completion(self, **kwargs):
        self.prompts.append(kwargs)
        return {"choices": [{"message": {"content": self.answer}}]}


class SlowModel(FakeModel):
    def create_chat_completion(self, **kwargs):
        time.sleep(1.2)
        return super().create_chat_completion(**kwargs)


class SequenceModel(FakeModel):
    def __init__(self, answers):
        super().__init__()
        self.answers = list(answers)

    def create_chat_completion(self, **kwargs):
        self.prompts.append(kwargs)
        answer = self.answers.pop(0)
        return {"choices": [{"message": {"content": answer}}]}


class MetadataContext:
    def invocation_metadata(self):
        return (("x-chat-current-user", "Alex"),)


class InferenceTests(unittest.TestCase):
    def test_context_is_bounded_and_recent_messages_are_kept(self):
        messages = ["old " + ("x" * 5000), "recent conversation detail"]
        bounded = _bound_messages(messages, max_chars=100)
        self.assertLessEqual(sum(len(line) for line in bounded), 100)
        self.assertIn("recent conversation detail", bounded[-1])

    def test_smart_replies_returns_three_unique_grounded_prompted_answers(self):
        model = FakeModel("1) Check the deployment logs.\n2) Confirm the 5 PM meeting.\n3) Share the CI failure details.")
        inference = ChatInference(model)
        replies = inference.smart_replies(
            ["Alice: deployment demo", "Bob: CI failure"],
            "Can you check the logs?",
            "Chat",
        )
        self.assertEqual(len(replies), 3)
        self.assertTrue(all(reply.strip() for reply in replies))
        self.assertEqual(
            replies,
            [
                "Check the deployment logs.",
                "Confirm the 5 PM meeting.",
                "Share the CI failure details.",
            ],
        )
        messages = model.prompts[0]["messages"]
        system_prompt, user_message = messages[0]["content"], messages[1]["content"]
        self.assertIn("deployment demo", system_prompt)
        self.assertEqual(user_message, "Can you check the logs?")
        self.assertEqual(user_message.count("Can you check the logs?"), 1)
        self.assertIn("casual team chat", system_prompt)
        self.assertIn("18 words or fewer", system_prompt)

    def test_smart_reply_prompt_deduplicates_latest_history_message(self):
        system_prompt, user_message = format_smart_reply(
            "Project chat",
            ["Sam: I can test the build.", "Priya: Can you check the logs?"],
            "Can you check the logs?",
            current_user="Alex",
        )
        self.assertEqual(user_message, "Can you check the logs?")
        self.assertEqual(user_message.count("Can you check the logs?"), 1)
        self.assertNotIn("Can you check the logs?", system_prompt)
        self.assertIn("Sam: I can test the build.", system_prompt)
        self.assertIn("You draft replies as Alex, the signed-in user", system_prompt)
        self.assertIn("ask if ownership is unclear", system_prompt)
        self.assertIn("three distinct, standalone options", system_prompt)
        self.assertIn("Incoming message: hi hows things", system_prompt)

    def test_greeting_is_the_user_message_once_and_example_is_in_system(self):
        system_prompt, user_message = format_smart_reply(
            "Project chat",
            ["Priya: hi hows things"],
            "hi hows things",
            current_user="Alex",
        )
        self.assertEqual(user_message, "hi hows things")
        self.assertEqual(user_message.count("hi hows things"), 1)
        self.assertIn('"replies":["Good, thanks! How about you?"', system_prompt)
        self.assertIn('"Doing okay - how are you?"', system_prompt)
        self.assertIn('"Pretty good! How\'s your day going?"', system_prompt)

    def test_llm_service_uses_signed_in_user_metadata_for_reply_voice(self):
        model = FakeModel("1) Sure, I can check.\n2) Which error should I check?\n3) Send me the failing test.")
        service = LLMServicer(ChatInference(model))
        response = service.GetSmartReplies(
            llm_pb2.SmartReplyRequest(
                chat_history=["Sam: The test failed.", "Sam: Can you check the logs?"],
                current_message="Can you check the logs?",
                context_title="Project chat",
            ),
            MetadataContext(),
        )
        self.assertTrue(response.success, response.error)
        messages = model.prompts[0]["messages"]
        self.assertIn("You draft replies as Alex, the signed-in user", messages[0]["content"])
        self.assertEqual(messages[1]["role"], "user")
        self.assertEqual(messages[1]["content"], "Can you check the logs?")

    def test_smart_replies_accepts_structured_json_output(self):
        model = FakeModel('{"replies":["I can check that.","What should I focus on?","Send me the details."]}')
        replies = ChatInference(model).smart_replies(
            ["Sam: Please review this."], "Can you take a look?"
        )
        self.assertEqual(replies, ["I can check that.", "What should I focus on?", "Send me the details."])
        self.assertEqual(model.prompts[0]["response_format"], {"type": "json_object"})

    def test_smart_reply_retries_once_after_invalid_format(self):
        model = SequenceModel([
            "1) Sure!\n2) Sure.\n3) This is a duplicate.",
            "1) I can check the logs now.\n2) Which error should I focus on?\n3) Send me the latest build.",
        ])
        replies = ChatInference(model).smart_replies(
            ["Priya: The build is failing in CI."],
            "Can you check the logs?",
        )
        self.assertEqual(len(model.prompts), 2)
        self.assertIn("previous response failed validation", model.prompts[1]["messages"][0]["content"])
        self.assertEqual(model.prompts[1]["messages"][1]["content"], "Can you check the logs?")
        self.assertEqual(replies[0], "I can check the logs now.")

    def test_smart_reply_does_not_pad_or_retry_more_than_once(self):
        model = SequenceModel(["1) Same reply.\n2) Same reply!", "Still only one reply."])
        inference = ChatInference(model)
        with self.assertRaisesRegex(RuntimeError, "three distinct"):
            inference.smart_replies(["Sam: Please review the draft."], "Can you review it?")
        self.assertEqual(len(model.prompts), 2)
        self.assertEqual(
            _parse_smart_replies("1) Sure.\n2) Which error?\n3) Send the logs."),
            ["Sure.", "Which error?", "Send the logs."],
        )
        self.assertIsNone(_parse_smart_replies("1) Sure.\n2) Sure!\n3) Thanks."))
        self.assertIsNone(_parse_smart_replies("Sure.\n2) Which error?\n3) Send the logs."))
        self.assertIsNone(_parse_smart_replies("1) Sure.\n3) Which error?\n2) Send the logs."))
        self.assertEqual(
            _parse_smart_replies("Replies:\n1) Sure.\n2) Which error?\n3) Send the logs."),
            ["Sure.", "Which error?", "Send the logs."],
        )
        self.assertEqual(
            _parse_smart_replies("\n1. Sure.\n\n2. Which error?\n3. Send the logs."),
            ["Sure.", "Which error?", "Send the logs."],
        )
        self.assertIsNone(
            _parse_smart_replies(
                "1) I can review the complete final draft after I finish the deployment work later this afternoon after the build.\n"
                "2) Which section matters most?\n3) Send me the link."
            )
        )

    def test_smart_replies_require_a_text_message(self):
        model = FakeModel()
        with self.assertRaisesRegex(ValueError, "require a text message"):
            ChatInference(model).smart_replies(["Sam: [File Attachment]"], "")
        self.assertEqual(model.prompts, [])

    def test_mock_fallback_when_no_model(self):
        inference = ChatInference(model=None, is_mock=True)
        replies = inference.smart_replies(["Alice: hello"], "How are you?")
        self.assertEqual(len(replies), 3)
        summary = inference.summarize(["Alice: status update"])
        self.assertIn("discussion", summary.lower())

    def test_summarize_includes_personalized_user_and_guidelines(self):
        model = FakeModel("• Admin asked you to review the deployment logs.")
        inference = ChatInference(model)
        summary = inference.summarize(
            ["Admin: please review deployment logs", "Ajay: on it"],
            context_title="DevOps",
            current_user="Ajay",
        )
        self.assertIn("Admin asked you", summary)
        prompt = model.prompts[0]["messages"][1]["content"]
        self.assertIn('- You: "on it"', prompt)
        self.assertIn('- Admin: "please review deployment logs"', prompt)
        self.assertIn('Refer to Ajay as "You"', prompt)
        self.assertIn("Write the summary paragraph now:", prompt)

        # Verify header stripping
        model2 = FakeModel("Summary for Ajay: You spoke with Admin.")
        inf2 = ChatInference(model2)
        s2 = inf2.summarize(["Admin: hi"], current_user="Ajay")
        self.assertEqual(s2, "You spoke with Admin.")

        # Verify multi-line response joins into continuous informative prose
        model3 = FakeModel("Alice asked for the staging report.\nYou confirmed that all checks passed.")
        inf3 = ChatInference(model3)
        s3 = inf3.summarize(["Alice: check report", "Ajay: all good"], current_user="Ajay")
        self.assertEqual(s3, "Alice asked for the staging report. You confirmed that all checks passed.")

        # Verify repeated sentence loop deduplication
        model4 = FakeModel("Alice asked you again. Alice asked you again. Alice asked you again.")
        inf4 = ChatInference(model4)
        s4 = inf4.summarize(["Alice: hi"], current_user="Ajay")
        self.assertEqual(s4, "Alice asked you again.")

    def test_summarize_bounds_to_previous_10_messages(self):
        model = FakeModel("You and Alice discussed project updates.")
        inference = ChatInference(model)
        messages = [f"Alice: message {i}" for i in range(15)]
        inference.summarize(messages, context_title="Test", current_user="Bob")
        prompt = model.prompts[0]["messages"][1]["content"]
        self.assertNotIn('"message 0"', prompt)
        self.assertNotIn('"message 4"', prompt)
        self.assertIn('"message 5"', prompt)
        self.assertIn('"message 14"', prompt)

    def test_inference_reports_timeout(self):
        with patch.dict(os.environ, {"MODEL_TIMEOUT_SECONDS": "1"}):
            inference = ChatInference(SlowModel())
            with self.assertRaisesRegex(RuntimeError, "exceeded timeout limit"):
                inference.summarize(["Alice: there is a 5 PM meeting"], "Chat")


if __name__ == "__main__":
    unittest.main()
