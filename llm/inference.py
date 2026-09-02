"""
LLM inference engine.

Milestone 1: Rule-based mock responses (no model needed).
The actual model loading code is commented out below.

To enable a real LLM, uncomment one of the model backends and set
the MODEL_PATH / MODEL_NAME environment variable.
"""

import logging
import os
import re
from typing import List

from llm.prompts import (
    format_context_suggestion,
    format_smart_reply,
    format_summarize,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Model configuration (uncomment ONE backend)
# ─────────────────────────────────────────────────────────────────────────────

# MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.gguf")
# MODEL_NAME = os.environ.get("MODEL_NAME", "microsoft/phi-2")

# --- Backend A: llama.cpp (CPU-optimised, GGUF format) ---
# from llama_cpp import Llama
# _model = Llama(model_path=MODEL_PATH, n_ctx=2048, n_threads=4, verbose=False)
#
# def _generate(prompt: str, max_tokens: int = 256) -> str:
#     out = _model(prompt, max_tokens=max_tokens, stop=["\n\n"])
#     return out["choices"][0]["text"].strip()

# --- Backend B: Hugging Face Transformers (CPU) ---
# from transformers import AutoTokenizer, AutoModelForCausalLM
# import torch
# _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
# _model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.float32)
# _model.eval()
#
# def _generate(prompt: str, max_tokens: int = 256) -> str:
#     inputs = _tokenizer(prompt, return_tensors="pt")
#     with torch.no_grad():
#         out = _model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
#     return _tokenizer.decode(out[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True).strip()

# --- Backend C: Ollama REST API ---
# import requests
# OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
# OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "phi3")
#
# def _generate(prompt: str, max_tokens: int = 256) -> str:
#     resp = requests.post(
#         f"{OLLAMA_URL}/api/generate",
#         json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False,
#               "options": {"num_predict": max_tokens}},
#         timeout=60,
#     )
#     resp.raise_for_status()
#     return resp.json()["response"].strip()

# ─────────────────────────────────────────────────────────────────────────────
# Mock fallback (used when no model backend is configured)
# ─────────────────────────────────────────────────────────────────────────────

USE_MOCK = True  # Set False when a real model backend is uncommented above


def _generate(prompt: str, max_tokens: int = 256) -> str:  # noqa: F811 (shadow)
    """Mock generator — returns rule-based text."""
    return "[MOCK] LLM not configured. See llm/inference.py to enable a model."


# ─────────────────────────────────────────────────────────────────────────────
# Feature implementations
# ─────────────────────────────────────────────────────────────────────────────

_QUESTION_REPLIES  = ["Sure, sounds good!", "Yes, I'll be there.", "Can we discuss this further?"]
_MEETING_REPLIES   = ["I'll add it to my calendar.", "Works for me!", "Can we push it by 30 minutes?"]
_HELP_REPLIES      = ["I'm on it!", "Let me check and get back to you.", "Happy to help."]
_GENERIC_REPLIES   = ["Got it, thanks!", "Will do.", "Sounds good — let me know if you need anything else."]


def _mock_smart_replies(current_message: str) -> List[str]:
    msg = current_message.lower()
    if "?" in msg:
        return _QUESTION_REPLIES
    if any(w in msg for w in ("meet", "meeting", "call", "pm", "am", "schedule")):
        return _MEETING_REPLIES
    if any(w in msg for w in ("help", "issue", "problem", "error", "fail", "bug")):
        return _HELP_REPLIES
    return _GENERIC_REPLIES


def _mock_summarize(messages: List[str], channel_name: str) -> str:
    n = len(messages)
    participants = set()
    for m in messages:
        if ": " in m:
            participants.add(m.split(": ")[0])
    return (
        f"Summary of #{channel_name} ({n} messages):\n"
        + (f"  - Participants: {', '.join(sorted(participants))}\n" if participants else "")
        + f"  - {n} messages exchanged.\n"
        + "  - (Enable a real LLM in llm/inference.py for intelligent summaries.)"
    )


def _mock_context_suggestion(recent_messages: List[str], channel_name: str, current_user: str) -> str:
    msgs_text = " ".join(recent_messages).lower()
    if any(w in msgs_text for w in ("deploy", "build", "ci", "pipeline")):
        return f"Suggested action: Review the deployment logs and share findings in #{channel_name}."
    if any(w in msgs_text for w in ("meet", "agenda", "schedule")):
        return "Suggested action: Confirm attendance and share the meeting agenda."
    if any(w in msgs_text for w in ("bug", "error", "issue", "fail")):
        return "Suggested action: Reproduce the issue locally and open a bug report."
    return f"Suggested action: Continue the discussion and summarise next steps in #{channel_name}."


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_smart_replies(
    recent_messages: List[str], current_message: str, channel_name: str
) -> List[str]:
    if USE_MOCK:
        return _mock_smart_replies(current_message)
    prompt = format_smart_reply(channel_name, recent_messages, current_message)
    raw = _generate(prompt, max_tokens=128)
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    return lines[:3] if lines else [raw]


def summarize_conversation(messages: List[str], channel_name: str) -> str:
    if USE_MOCK:
        return _mock_summarize(messages, channel_name)
    prompt = format_summarize(channel_name, messages)
    return _generate(prompt, max_tokens=256)


def get_context_suggestion(
    recent_messages: List[str], channel_name: str, current_user: str
) -> str:
    if USE_MOCK:
        return _mock_context_suggestion(recent_messages, channel_name, current_user)
    prompt = format_context_suggestion(channel_name, current_user, recent_messages)
    return _generate(prompt, max_tokens=128)
