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
You are an executive assistant that writes brief, grounded chat summaries in one concise paragraph.

Chat: {context_title}

Messages:
{messages}

Instructions:
Write a concise 1-paragraph summary (2-3 sentences max) from the perspective of {current_user}:
- Refer to {current_user} as "You", and other participants by name.
- Summarize what you and others discussed, requested, or confirmed.
- Do NOT output dialogue or script lines (never write "Name: message").
- Do NOT list isolated greetings or repeat words.
- Rely ONLY on statements directly present above. Do NOT invent events, topics, or plans.

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


def format_summarize(context_title: str, messages: list, current_user: str = "you") -> str:
    user = (current_user or "you").strip()

    # Clean and group messages to prevent raw repetition and spam loops
    grouped = []
    for msg in (messages or []):
        m = str(msg).strip()
        if not m:
            continue
        colon_idx = m.find(":")
        if colon_idx != -1:
            sender = m[:colon_idx].strip()
            content = m[colon_idx + 1:].strip()
        else:
            sender = "User"
            content = m

        if not content:
            continue

        if grouped and grouped[-1]["sender"].lower() == sender.lower():
            # If exact same message repeated by same sender, don't spam it
            if content.lower() != grouped[-1]["last_content"].lower():
                grouped[-1]["content"] += f", {content}"
                grouped[-1]["last_content"] = content
        else:
            grouped.append({"sender": sender, "content": content, "last_content": content})

    formatted_msgs = [f"{g['sender']}: {g['content']}" for g in grouped]
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
