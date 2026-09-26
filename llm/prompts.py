"""
Prompt templates for LLM features.

These templates ensure the model stays grounded in the actual conversation context
and does not invent meeting times, dates, decisions, or assignments.
"""


SMART_REPLY_PROMPT = """\
You are a helpful chat assistant.

Recent conversation in {context_title}:
{context}

The last message is:
{current_message}

Generate exactly 3 different short reply suggestions, one per line, with no heading or numbering.
Keep each reply under 15 words. At least one should directly address the last message.
Prefer an acknowledgement, a relevant clarifying question, or an offer to help.
Never propose a new time or a change to the team's plan unless that exact change
was discussed in the conversation.
Replies:"""


SUMMARIZE_PROMPT = """\
You summarize team chat accurately. Use only statements present in the messages.

Chat: {context_title}

Messages:
{messages}

Write a concise bullet-point summary of the key topics, confirmed decisions, action items,
and unresolved questions. Do not turn suggestions into decisions.
Summary:"""


CONTEXT_SUGGESTION_PROMPT = """\
You help a team member make progress based only on the supplied team chat.

Chat: {context_title}
Current user: {current_user}

Recent messages:
{context}

Based on the conversation, suggest one concrete next step relevant to {current_user}.
Do not invent an assignment or deadline. Keep it to one sentence.
Suggestion:"""


def format_smart_reply(context_title: str, recent_messages: list, current_message: str) -> str:
    context = "\n".join(recent_messages[-15:]) if recent_messages else "(no prior context)"
    return SMART_REPLY_PROMPT.format(
        context_title=context_title or "Chat",
        context=context,
        current_message=current_message,
    )


def format_summarize(context_title: str, messages: list) -> str:
    return SUMMARIZE_PROMPT.format(
        context_title=context_title or "Chat",
        messages="\n".join(messages),
    )


def format_context_suggestion(context_title: str, current_user: str, recent_messages: list) -> str:
    context = "\n".join(recent_messages[-15:])
    return CONTEXT_SUGGESTION_PROMPT.format(
        context_title=context_title or "Chat",
        current_user=current_user,
        context=context,
    )
