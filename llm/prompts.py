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
Recent conversation in {context_title}:
{messages}

Write a natural, conversational summary of what happened for the user (You):
- Always refer to the user as "You" and other participants by their names.
- Describe the conversation flow naturally (e.g. what you commented, what others replied, and the outcome).
- Do not mention who the chat is with (never say "You are chatting with...").
- Do not list isolated single words. Summarize the meaning of the exchanges.
- Keep it natural, human-like, and concise.

Summary in one paragraph:"""


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


def format_summarize(context_title: str, messages: list, current_user: str = "you") -> str:
    user = (current_user or "you").strip()
    user_lower = user.lower()

    formatted_msgs = []
    for msg in (messages or []):
        m = str(msg).strip()
        colon_idx = m.find(":")
        if colon_idx != -1:
            sender = m[:colon_idx].strip()
            rest = m[colon_idx + 1:].strip()
            if sender.lower() == user_lower:
                formatted_msgs.append(f"You: {rest}")
            else:
                formatted_msgs.append(f"{sender}: {rest}")
        else:
            formatted_msgs.append(m)

    msg_block = "\n".join(formatted_msgs) if formatted_msgs else "(no messages)"
    return SUMMARIZE_PROMPT.format(
        context_title=context_title or "Chat",
        current_user=user,
        messages=msg_block,
    )


def format_context_suggestion(context_title: str, current_user: str, recent_messages: list) -> str:
    context = "\n".join(recent_messages[-15:])
    return CONTEXT_SUGGESTION_PROMPT.format(
        context_title=context_title or "Chat",
        current_user=current_user,
        context=context,
    )
