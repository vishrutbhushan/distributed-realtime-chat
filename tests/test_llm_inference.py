"""
LLM prompt, bounding, and response handling tests that do not require model downloads.
"""

import os
import time
import unittest
from unittest.mock import patch

from llm.inference import ChatInference, _bound_messages


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


class InferenceTests(unittest.TestCase):
    def test_context_is_bounded_and_recent_messages_are_kept(self):
        messages = ["old " + ("x" * 5000), "recent conversation detail"]
        bounded = _bound_messages(messages, max_chars=100)
        self.assertLessEqual(sum(len(line) for line in bounded), 100)
        self.assertIn("recent conversation detail", bounded[-1])

    def test_smart_replies_returns_three_unique_grounded_prompted_answers(self):
        model = FakeModel("1. Check the deployment logs.\n- Confirm the 5 PM meeting.\n3) Share the CI failure details.")
        inference = ChatInference(model)
        replies = inference.smart_replies(
            ["Alice: deployment demo", "Bob: CI failure"],
            "Can you check the logs?",
            "Chat",
        )
        self.assertEqual(len(replies), 3)
        self.assertTrue(all(reply.strip() for reply in replies))
        prompt = model.prompts[0]["messages"][1]["content"]
        self.assertIn("deployment demo", prompt)
        self.assertIn("Can you check the logs?", prompt)

    def test_mock_fallback_when_no_model(self):
        inference = ChatInference(model=None, is_mock=True)
        replies = inference.smart_replies(["Alice: hello"], "How are you?")
        self.assertEqual(len(replies), 3)
        summary = inference.summarize(["Alice: status update"])
        self.assertIn("Discussion", summary)

    def test_inference_reports_timeout(self):
        with patch.dict(os.environ, {"MODEL_TIMEOUT_SECONDS": "1"}):
            inference = ChatInference(SlowModel())
            with self.assertRaisesRegex(RuntimeError, "exceeded timeout limit"):
                inference.summarize(["Alice: there is a 5 PM meeting"], "Chat")


if __name__ == "__main__":
    unittest.main()
