"""
LLM inference engine.

Loads a local CPU-optimized GGUF model (Qwen2.5-1.5B-Instruct) via llama-cpp-python,
with concurrency throttling via BoundedSemaphore, timeout guards, and graceful
mock fallback if no local model is installed.
"""

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
    """Keep the newest conversation lines within a conservative prompt character budget."""
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
    """Shared local LLM inference instance; bounded calls run one at a time."""

    def __init__(self, model=None, is_mock: bool = False):
        self._model = model
        self.is_mock = is_mock
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-llm")
        self._slot = threading.BoundedSemaphore(1)
        self._timeout_seconds = max(1, int(os.environ.get("MODEL_TIMEOUT_SECONDS", "60")))

    @staticmethod
    def _detect_gpu_layers() -> int:
        """
        Detect hardware acceleration capability:
        - Explicit MODEL_N_GPU_LAYERS override
        - Apple Silicon Metal (macOS M1/M2/M3/M4) -> -1 (all layers)
        - NVIDIA CUDA (Linux / Windows) -> -1 (all layers)
        - Fallback: 0 (CPU)
        """
        env_val = os.environ.get("MODEL_N_GPU_LAYERS")
        if env_val is not None:
            try:
                layers = int(env_val)
                logger.info("[LLM] Hardware acceleration configured via MODEL_N_GPU_LAYERS=%d", layers)
                return layers
            except ValueError:
                pass

        # 1. Apple Silicon Metal (macOS)
        import platform
        if platform.system() == "Darwin" and platform.machine() in ("arm64", "aarch64"):
            logger.info("[LLM] Apple Silicon detected (Metal acceleration enabled).")
            return -1

        # 2. NVIDIA CUDA check via torch if installed
        try:
            import torch
            if torch.cuda.is_available():
                logger.info("[LLM] NVIDIA CUDA detected via torch: %s", torch.cuda.get_device_name(0))
                return -1
            if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                logger.info("[LLM] Apple Silicon MPS detected via torch.")
                return -1
        except Exception:
            pass

        # 3. NVIDIA CUDA check via nvidia-smi command
        import shutil
        if shutil.which("nvidia-smi") is not None:
            logger.info("[LLM] NVIDIA GPU detected via nvidia-smi (CUDA acceleration enabled).")
            return -1

        # 4. Linux /proc/driver/nvidia check
        if os.path.exists("/proc/driver/nvidia"):
            logger.info("[LLM] NVIDIA driver detected via /proc/driver/nvidia (CUDA acceleration enabled).")
            return -1

        logger.info("[LLM] No GPU detected; using CPU inference.")
        return 0

    @classmethod
    def from_env(cls):
        """
        Load model specified by MODEL_PATH.
        Auto-detects GPU (CUDA / Apple Silicon Metal) and offloads layers if available.
        Falls back cleanly to CPU or rule-based mock engine.
        """
        model_path = Path(os.environ.get("MODEL_PATH", DEFAULT_MODEL_PATH))
        if not model_path.is_file():
            logger.warning("[LLM] Model not found at %s. Falling back to rule-based mock engine.", model_path)
            return cls(model=None, is_mock=True)

        try:
            from llama_cpp import Llama
        except ImportError as exc:
            logger.warning("[LLM] llama-cpp-python not installed (%s). Falling back to mock engine.", exc)
            return cls(model=None, is_mock=True)

        n_ctx = int(os.environ.get("MODEL_N_CTX", "4096"))
        n_threads = int(os.environ.get("MODEL_THREADS", "4"))
        n_gpu_layers = cls._detect_gpu_layers()

        logger.info("[LLM] Loading GGUF model=%s (ctx=%d, threads=%d, gpu_layers=%d)...",
                    model_path, n_ctx, n_threads, n_gpu_layers)
        try:
            model = Llama(
                model_path=str(model_path),
                n_ctx=n_ctx,
                n_threads=n_threads,
                n_gpu_layers=n_gpu_layers,
                n_batch=256,
                verbose=False,
            )
            device_mode = "GPU (accelerated)" if n_gpu_layers != 0 else "CPU"
            logger.info("[LLM] Local GGUF model successfully loaded into memory (%s mode).", device_mode)
        except Exception as exc:
            if n_gpu_layers != 0:
                logger.warning("[LLM] Failed loading with GPU layers (%s). Falling back to CPU...", exc)
                model = Llama(
                    model_path=str(model_path),
                    n_ctx=n_ctx,
                    n_threads=n_threads,
                    n_gpu_layers=0,
                    n_batch=256,
                    verbose=False,
                )
                logger.info("[LLM] Local GGUF model loaded in CPU fallback mode.")
            else:
                raise

        return cls(model=model, is_mock=False)

    def _complete(self, prompt: str, max_tokens: int, temperature: float) -> str:
        if self.is_mock or self._model is None:
            return self._mock_complete(prompt)

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
            raise RuntimeError("Local LLM is busy processing another request; retry shortly")
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
                f"Local model inference exceeded timeout limit of {self._timeout_seconds} seconds"
            ) from exc

        answer = response["choices"][0]["message"]["content"]
        answer = (answer or "").strip()
        if not answer:
            raise RuntimeError("The local model returned an empty response")
        return answer

    def _mock_complete(self, prompt: str) -> str:
        """Rule-based mock generator for development and environments without GGUF weights."""
        if "generate exactly 3" in prompt.lower():
            return "Sounds good, thanks for confirming!\nI'll check the details and report back.\nLet me know if you need any assistance."
        if "summary" in prompt.lower():
            return "• Discussion ongoing in chat.\n• Key points shared between participants.\n• Action items logged."
        return "Suggested action: Follow up on the latest message and confirm details."

    def smart_replies(
        self, recent_messages: List[str], current_message: str, context_title: str = "Chat"
    ) -> List[str]:
        prompt = format_smart_reply(
            context_title,
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
            # Pad with sensible fallbacks if model output had fewer than 3 lines
            defaults = ["Sounds good!", "I'll look into it.", "Understood, thanks!"]
            for d in defaults:
                if len(replies) >= 3:
                    break
                if d.lower() not in {item.lower() for item in replies}:
                    replies.append(d)
        return replies[:3]

    def summarize(self, messages: List[str], context_title: str = "Chat") -> str:
        prompt = format_summarize(context_title, _bound_messages(messages))
        return self._complete(prompt, max_tokens=256, temperature=0.1)

    def suggest(
        self, recent_messages: List[str], context_title: str = "Chat", current_user: str = "user"
    ) -> str:
        prompt = format_context_suggestion(
            context_title,
            current_user,
            _bound_messages(recent_messages),
        )
        return self._complete(prompt, max_tokens=128, temperature=0.15)
