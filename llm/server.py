"""
LLM gRPC server.

Exposes LLMService defined in proto/llm.proto.
Runs on a dedicated node/container and loads the local CPU model before
opening its gRPC port.
"""

import logging
import os
import sys
import uuid
from concurrent import futures

import grpc

# Generated stubs
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GENERATED = os.path.join(_ROOT, "generated")
sys.path.insert(0, _GENERATED)
sys.path.insert(0, _ROOT)

import llm_pb2
import llm_pb2_grpc
from llm.inference import ChatInference

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] LLM %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("LLM_PORT", "50060"))


class LLMServicer(llm_pb2_grpc.LLMServiceServicer):

    def __init__(self, inference: ChatInference):
        self.inference = inference

    def GetLLMAnswer(self, request, context):
        logger.info("[LLM] GetLLMAnswer feature=%s", request.feature)
        rid = request.request_id or str(uuid.uuid4())
        feature = (request.feature or "").upper()

        try:
            if feature == "SMART_REPLY":
                replies = self.inference.smart_replies(
                    request.context.splitlines() if request.context else [],
                    request.query,
                    "Chat",
                )
                result = "\n".join(replies)
            elif feature == "SUMMARIZE":
                result = self.inference.summarize(
                    request.context.splitlines() if request.context else [],
                    "Chat",
                )
            elif feature == "SUGGEST":
                result = self.inference.suggest(
                    request.context.splitlines() if request.context else [],
                    "Chat",
                    "user",
                )
            else:
                replies = self.inference.smart_replies([], request.query, "Chat")
                result = replies[0] if replies else "No response generated."

            return llm_pb2.LLMResponse(request_id=rid, result=result, success=True)
        except Exception as exc:
            logger.error("[LLM] GetLLMAnswer error: %s", exc)
            return llm_pb2.LLMResponse(request_id=rid, success=False, error=str(exc))

    def GetSmartReplies(self, request, context):
        rid = request.request_id or str(uuid.uuid4())
        # Support both chat_history and recent_messages
        history = []
        if hasattr(request, "chat_history") and request.chat_history:
            history = list(request.chat_history)
        elif hasattr(request, "recent_messages") and request.recent_messages:
            history = list(request.recent_messages)

        title = getattr(request, "context_title", "") or getattr(request, "channel_name", "") or "Chat"
        logger.info("[LLM] SmartReplies title='%s' msgs=%d", title, len(history))

        try:
            suggestions = self.inference.smart_replies(
                history,
                request.current_message,
                title,
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
        # Support both chat_history and messages
        history = []
        if hasattr(request, "chat_history") and request.chat_history:
            history = list(request.chat_history)
        elif hasattr(request, "messages") and request.messages:
            history = list(request.messages)

        title = getattr(request, "context_title", "") or getattr(request, "channel_name", "") or "Chat"
        user = getattr(request, "current_user", "") or "you"
        logger.info("[LLM] Summarize title='%s' msgs=%d user='%s'", title, len(history), user)

        try:
            summary = self.inference.summarize(history, title, current_user=user)
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
        history = []
        if hasattr(request, "chat_history") and request.chat_history:
            history = list(request.chat_history)
        elif hasattr(request, "recent_messages") and request.recent_messages:
            history = list(request.recent_messages)

        title = getattr(request, "context_title", "") or getattr(request, "channel_name", "") or "Chat"
        user = getattr(request, "current_user", "user") or "user"
        logger.info("[LLM] ContextSuggestion title='%s' user='%s'", title, user)

        try:
            suggestion = self.inference.suggest(history, title, user)
            return llm_pb2.ContextSuggestionResponse(
                request_id=rid, suggestion=suggestion, success=True
            )
        except Exception as exc:
            logger.error("[LLM] ContextSuggestion error: %s", exc)
            return llm_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error=str(exc)
            )


def serve():
    inference = ChatInference.from_env()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(inference), server)
    addr = f"[::]:{PORT}"
    server.add_insecure_port(addr)
    server.start()
    logger.info("[LLM] Server listening on %s", addr)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
