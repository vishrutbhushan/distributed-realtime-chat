"""CPU local inference for chat assistance using a GGUF instruct model."""

import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from typing import Iterable, List

from llm.prompts import (
    format_context_suggestion,
    format_smart_reply,
    format_summarize,
)

logger = logging.getLogger(__name__)

DEFAULT_MODEL_PATH = "/models/qwen2.5-1.5b-instruct-q4_k_m.gguf"
MAX_CONTEXT_CHARS = 8000


def _bound_messages(messages: Iterable[str], max_chars: int = MAX_CONTEXT_CHARS) -> List[str]:
    """Keep the newest conversation lines within a conservative prompt bound."""
    bounded = []
    used = 0
    for message in reversed(list(messages)):
        line = str(message)
        remaining = max_chars - used
        if remaining <= 0:
            break
        if len(line) > remaining:
            line = line[-remaining:]
        bounded.append(line)
        used += len(line) + 1
    bounded.reverse()
    return bounded


class ChatInference:
    """One shared model instance; bounded calls run one at a time."""

    def __init__(self, model):
        self._model = model
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-llm")
        self._slot = threading.BoundedSemaphore(1)
        self._timeout_seconds = max(1, int(os.environ.get("MODEL_TIMEOUT_SECONDS", "60")))

    @classmethod
    def from_env(cls):
        model_path = Path(os.environ.get("MODEL_PATH", DEFAULT_MODEL_PATH))
        if not model_path.is_file():
            raise FileNotFoundError(f"Local LLM model not found: {model_path}")
        try:
            from llama_cpp import Llama
        except ImportError as exc:
            raise RuntimeError(
                "llama-cpp-python is required; install the CPU model dependencies"
            ) from exc

        n_ctx = int(os.environ.get("MODEL_N_CTX", "4096"))
        n_threads = int(os.environ.get("MODEL_THREADS", "4"))
        logger.info(
            "[LLM] Loading model=%s context=%d threads=%d",
            model_path,
            n_ctx,
            n_threads,
        )
        model = Llama(
            model_path=str(model_path),
            n_ctx=n_ctx,
            n_threads=n_threads,
            n_batch=256,
            verbose=False,
        )
        logger.info("[LLM] Model loaded")
        return cls(model)

    def _complete(self, prompt: str, max_tokens: int, temperature: float) -> str:
        messages = [
            {
                "role": "system",
                "content": (
                    "You assist people collaborating in a team chat. Ground every answer "
                    "in the supplied conversation. Do not invent decisions, events, or facts."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        if not self._slot.acquire(blocking=False):
            raise RuntimeError("Local model is busy; retry shortly")
        try:
            future = self._executor.submit(
                self._model.create_chat_completion,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
        except Exception:
            self._slot.release()
            raise
        future.add_done_callback(lambda _: self._slot.release())
        try:
            response = future.result(timeout=self._timeout_seconds)
        except TimeoutError as exc:
            raise RuntimeError(
                f"Local model inference exceeded {self._timeout_seconds} seconds"
            ) from exc
        answer = response["choices"][0]["message"]["content"]
        answer = (answer or "").strip()
        if not answer:
            raise RuntimeError("The local model returned an empty response")
        return answer

    def smart_replies(
        self, recent_messages: List[str], current_message: str, channel_name: str
    ) -> List[str]:
        prompt = format_smart_reply(
            channel_name,
            _bound_messages(recent_messages),
            current_message,
        )
        raw = self._complete(prompt, max_tokens=160, temperature=0.25)
        replies = []
        for line in raw.splitlines():
            cleaned = re.sub(r"^\s*(?:\d+[.)]|[-*])\s*", "", line).strip()
            if cleaned and cleaned.lower() not in {item.lower() for item in replies}:
                replies.append(cleaned)
        if len(replies) < 3:
            raise RuntimeError("The local model returned fewer than three reply suggestions")
        return replies[:3]

    def summarize(self, messages: List[str], channel_name: str) -> str:
        prompt = format_summarize(channel_name, _bound_messages(messages))
        return self._complete(prompt, max_tokens=256, temperature=0.1)

    def suggest(
        self, recent_messages: List[str], channel_name: str, current_user: str
    ) -> str:
        prompt = format_context_suggestion(
            channel_name,
            current_user,
            _bound_messages(recent_messages),
        )
        return self._complete(prompt, max_tokens=128, temperature=0.15)
