"""
LLM gRPC server.

Exposes LLMService defined in proto/llm.proto.
Operates on a standalone instance and processes full chat history.
Actual model inference is in llm/inference.py (mocked by default, with CPU-optimized backends ready to uncomment).
"""

import logging
import os
import sys
import uuid
from concurrent import futures

import grpc

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GENERATED = os.path.join(_ROOT, "generated")
sys.path.insert(0, _GENERATED)
sys.path.insert(0, _ROOT)

import llm_pb2
import llm_pb2_grpc

from llm.inference import (
    get_context_suggestion,
    get_smart_replies,
    summarize_conversation,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] LLM %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("LLM_PORT", "50060"))


class LLMServicer(llm_pb2_grpc.LLMServiceServicer):

    def GetLLMAnswer(self, request, context):
        logger.info("[LLM] GetLLMAnswer feature=%s", request.feature)
        rid = request.request_id or str(uuid.uuid4())
        feature = request.feature.upper()

        try:
            if feature == "SMART_REPLY":
                replies = get_smart_replies([], request.query, "Chat")
                result = "\n".join(replies)
            elif feature == "SUMMARIZE":
                result = summarize_conversation(
                    request.context.splitlines(), "Chat"
                )
            elif feature == "SUGGEST":
                result = get_context_suggestion(
                    request.context.splitlines(), "Chat", "user"
                )
            else:
                result = get_smart_replies([], request.query, "Chat")[0]

            return llm_pb2.LLMResponse(request_id=rid, result=result, success=True)
        except Exception as exc:
            logger.error("[LLM] GetLLMAnswer error: %s", exc)
            return llm_pb2.LLMResponse(request_id=rid, success=False, error=str(exc))

    def GetSmartReplies(self, request, context):
        rid = request.request_id or str(uuid.uuid4())
        logger.info("[LLM] SmartReplies title=%s msgs=%d", request.context_title, len(request.chat_history))
        try:
            suggestions = get_smart_replies(
                list(request.chat_history),
                request.current_message,
                request.context_title or "Chat",
            )
            return llm_pb2.SmartReplyResponse(
                request_id=rid, suggestions=suggestions, success=True
            )
        except Exception as exc:
            logger.error("[LLM] SmartReplies error: %s", exc)
            return llm_pb2.SmartReplyResponse(
                request_id=rid, success=False, error=str(exc)
            )

    def SummarizeConversation(self, request, context):
        rid = request.request_id or str(uuid.uuid4())
        logger.info(
            "[LLM] Summarize title=%s msgs=%d",
            request.context_title, len(request.chat_history),
        )
        try:
            summary = summarize_conversation(
                list(request.chat_history), request.context_title or "Chat"
            )
            return llm_pb2.SummarizeResponse(
                request_id=rid, summary=summary, success=True
            )
        except Exception as exc:
            logger.error("[LLM] Summarize error: %s", exc)
            return llm_pb2.SummarizeResponse(
                request_id=rid, success=False, error=str(exc)
            )

    def GetContextSuggestion(self, request, context):
        rid = request.request_id or str(uuid.uuid4())
        logger.info("[LLM] ContextSuggestion title=%s", request.context_title)
        try:
            suggestion = get_context_suggestion(
                list(request.chat_history),
                request.context_title or "Chat",
                request.current_user,
            )
            return llm_pb2.ContextSuggestionResponse(
                request_id=rid, suggestion=suggestion, success=True
            )
        except Exception as exc:
            logger.error("[LLM] ContextSuggestion error: %s", exc)
            return llm_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error=str(exc)
            )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ("grpc.max_send_message_length",    64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    addr = f"[::]:{PORT}"
    server.add_insecure_port(addr)
    server.start()
    logger.info("[LLM] Server listening on %s", addr)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
