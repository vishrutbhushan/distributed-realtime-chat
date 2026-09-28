"""
LLM inference engine.

Loads a local CPU-optimized GGUF model (Qwen2.5-1.5B-Instruct) via llama-cpp-python,
with concurrency throttling via BoundedSemaphore, timeout guards, and graceful
mock fallback if no local model is installed.
"""

import logging
import json
import os
import re
import threading
import time
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
MAX_SMART_REPLY_WORDS = 18
SMART_REPLY_RESPONSE_FORMAT = {"type": "json_object"}
SMART_REPLY_GBNF = r'''
root ::= "{" ws "\"replies\"" ws ":" ws "[" ws string ws "," ws string ws "," ws string ws "]" ws "}"
string ::= "\"" chars "\""
chars ::= ([^"\\] | "\\" escape)*
escape ::= ["\\/bfnrt] | "u" hex hex hex hex
hex ::= [0-9a-fA-F]
ws ::= [ \t\n\r]*
'''


def _parse_smart_replies(raw: str) -> List[str] | None:
    """Parse constrained JSON replies, with legacy numbered-text support for mocks."""
    text = (raw or "").strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        candidate_replies = payload.get("replies")
        if not isinstance(candidate_replies, list) or len(candidate_replies) != 3:
            return None
        replies = [str(reply).strip() for reply in candidate_replies]
        if not all(replies):
            return None
    else:
        markers = list(re.finditer(r"(?:^|\n|\s)([1-3])[.)]\s+", text))
        if len(markers) != 3:
            return None

        replies = []
        for expected_number, marker in enumerate(markers, start=1):
            if int(marker.group(1)) != expected_number:
                return None
            end = markers[expected_number].start() if expected_number < 3 else len(text)
            cleaned = text[marker.end():end].strip(" \t\r\n\"'`“”‘’")
            if not cleaned:
                return None
            replies.append(cleaned)

    if any(len(reply.split()) > MAX_SMART_REPLY_WORDS for reply in replies):
            return None

    if len(replies) != 3:
        return None

    normalized = [re.sub(r"[^\w]+", " ", reply.casefold()).strip() for reply in replies]
    if any(not value for value in normalized) or len(set(normalized)) != 3:
        return None
    return replies


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

    def _complete(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        preserve_lines: bool = False,
        timeout_seconds: float | None = None,
        system_prompt: str | None = None,
        response_format: dict | None = None,
        grammar=None,
    ) -> str:
        if self.is_mock or self._model is None:
            mock_prompt = f"{system_prompt or ''}\n{prompt}"
            logger.info(
                "[LLM] completion_request mode=mock system_prompt=%s user_prompt=%s",
                json.dumps(system_prompt or "", ensure_ascii=False),
                json.dumps(prompt, ensure_ascii=False),
            )
            answer = self._mock_complete(mock_prompt)
            logger.info(
                "[LLM] completion_response mode=mock chars=%d output=%s",
                len(answer),
                json.dumps(answer, ensure_ascii=False),
            )
            return answer

        timeout_seconds = timeout_seconds or self._timeout_seconds

        messages = [
            {
                "role": "system",
                "content": system_prompt or (
                    "You assist people collaborating in a team chat. Ground every answer "
                    "in the supplied conversation. Do not invent decisions, events, or facts."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        logger.info(
            "[LLM] completion_request messages=%s response_format=%s max_tokens=%s temperature=%s",
            json.dumps(messages, ensure_ascii=False),
            json.dumps(response_format, ensure_ascii=False) if response_format else "none",
            max_tokens,
            temperature,
        )
        slot_timeout = min(float(timeout_seconds), 25.0)
        if not self._slot.acquire(timeout=slot_timeout):
            logger.error("[LLM] completion_rejected reason=model_busy")
            raise RuntimeError("Local LLM is busy processing another request; retry shortly")
        try:
            future = self._executor.submit(
                self._model.create_chat_completion,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_format=response_format,
                grammar=grammar,
            )
        except Exception:
            self._slot.release()
            raise

        future.add_done_callback(lambda _: self._slot.release())
        try:
            response = future.result(timeout=timeout_seconds)
        except TimeoutError as exc:
            logger.error(
                "[LLM] completion_timeout timeout_seconds=%s messages=%s",
                timeout_seconds,
                json.dumps(messages, ensure_ascii=False),
            )
            raise RuntimeError(
                f"Local model inference exceeded timeout limit of {timeout_seconds:g} seconds"
            ) from exc

        answer = response["choices"][0]["message"]["content"]
        answer = (answer or "").strip()
        logger.info(
            "[LLM] completion_response chars=%d output=%s",
            len(answer),
            json.dumps(answer, ensure_ascii=False),
        )
        if preserve_lines:
            if not answer:
                raise RuntimeError("The local model returned an empty response")
            return answer

        # Clean up any accidental model headers or intros
        answer = re.sub(r"^(?:(?:summary|recap)(?:\s+for\s+[^:\n]+)?:\s*)+", "", answer, flags=re.IGNORECASE).strip()
        answer = re.sub(r"^You(?:'re| are) currently chatting with [^.]+\.\s*", "", answer, flags=re.IGNORECASE).strip()

        # Clean bullet points / line breaks into clean continuous text
        lines = [re.sub(r"^[-*•]\s*", "", l.strip()) for l in answer.splitlines() if l.strip()]
        if lines:
            answer = " ".join(lines)

        # Remove repetitive looped sentences
        sentences = re.split(r"(?<=[.!?])\s+", answer)
        deduped = []
        for s in sentences:
            s_clean = s.strip()
            if not s_clean:
                continue
            if not deduped or s_clean.lower() != deduped[-1].lower():
                deduped.append(s_clean)
        if deduped:
            answer = " ".join(deduped)

        if not answer:
            raise RuntimeError("The local model returned an empty response")
        return answer

    def _mock_complete(self, prompt: str) -> str:
        """Rule-based mock generator for development and environments without GGUF weights."""
        if "three distinct, standalone options" in prompt.lower():
            return (
                "1) Sounds good, thanks for confirming!\n"
                "2) I'll check the details and report back.\n"
                "3) Let me know if you need any assistance."
            )
        if "summary" in prompt.lower():
            return "You and the team reviewed ongoing updates and confirmed active discussion points."
        return "Suggested action: Follow up on the latest message and confirm details."

    def smart_replies(
        self,
        recent_messages: List[str],
        current_message: str,
        context_title: str = "Chat",
        current_user: str = "the current user",
    ) -> List[str]:
        if not (current_message or "").strip():
            raise ValueError("Smart replies require a text message to reply to")

        started = time.monotonic()
        smart_reply_grammar = None
        if not self.is_mock and self._model is not None:
            try:
                from llama_cpp import LlamaGrammar
                smart_reply_grammar = LlamaGrammar.from_string(SMART_REPLY_GBNF)
            except ImportError:
                logger.debug("[LLM] llama_cpp unavailable; using parser validation only")
        for attempt in range(2):
            remaining = self._timeout_seconds - (time.monotonic() - started)
            if remaining <= 0:
                break

            system_prompt, latest_message = format_smart_reply(
                context_title,
                _bound_messages(recent_messages),
                current_message,
                current_user=current_user,
                retry=attempt == 1,
            )
            raw = self._complete(
                latest_message,
                max_tokens=160,
                temperature=0.25,
                preserve_lines=True,
                timeout_seconds=remaining,
                system_prompt=system_prompt,
                response_format=SMART_REPLY_RESPONSE_FORMAT,
                grammar=smart_reply_grammar,
            )
            logger.info(
                "[LLM] SmartReplies raw_output attempt=%d chars=%d output=%r",
                attempt + 1, len(raw), raw[:2000],
            )
            replies = _parse_smart_replies(raw)
            if replies is not None:
                return replies

        raise RuntimeError("The local model could not return three distinct, valid smart replies")

    def summarize(
        self, messages: List[str], context_title: str = "Chat", current_user: str = "you"
    ) -> str:
        if not messages:
            return "No messages in this chat to summarize yet."
        prompt = format_summarize(context_title, _bound_messages(messages), current_user=current_user)
        raw = self._complete(prompt, max_tokens=220, temperature=0.1)

        # Personalize pronouns: ensure current_user is addressed as "You" / "you"
        user = (current_user or "you").strip()
        if user and user.lower() != "you":
            raw = re.sub(rf"^(?:In this chat,\s*)?{re.escape(user)}\b", "You", raw, flags=re.IGNORECASE)
            raw = re.sub(rf"\b{re.escape(user)}\b", "you", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\byou's\b", "your", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\bYou was\b", "You were", raw)
            raw = re.sub(r"\byou was\b", "you were", raw)
            raw = re.sub(r"\bYou has\b", "You have", raw)
            raw = re.sub(r"\byou has\b", "you have", raw)

        return raw

    def suggest(
        self, recent_messages: List[str], context_title: str = "Chat", current_user: str = "user"
    ) -> str:
        prompt = format_context_suggestion(
            context_title,
            current_user,
            _bound_messages(recent_messages),
        )
        return self._complete(prompt, max_tokens=128, temperature=0.15)
