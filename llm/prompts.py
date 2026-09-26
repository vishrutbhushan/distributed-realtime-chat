"""
Prompt templates for LLM features.

These templates are ready to be used with any instruct-tuned LLM.
The inference.py module formats these templates and passes them to the model.

The active local model is configured through environment variables in
docker-compose.yml and docker/Dockerfile.llm.
"""


SMART_REPLY_PROMPT = """\
You are a concise assistant helping a person reply in a team chat. Use the surrounding
conversation to make each reply specific, natural, and consistent with what the team said.
Use only facts stated in the conversation. Do not invent or change times, plans,
owners, causes, decisions, or events. Do not claim that the current user completed
an action unless the conversation says so. When context is thin, keep replies neutral.

Recent conversation in #{channel_name}:
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

Channel: #{channel_name}

Messages:
{messages}

Write a concise bullet-point summary of the key topics, confirmed decisions, action items,
and unresolved questions. Do not turn suggestions into decisions.
Summary:"""


CONTEXT_SUGGESTION_PROMPT = """\
You help a team member make progress based only on the supplied team chat.

Channel: #{channel_name}
Current user: {current_user}

Recent messages:
{context}

Based on the conversation, suggest one concrete next step relevant to {current_user}.
Do not invent an assignment or deadline. Keep it to one sentence.
Suggestion:"""


def format_smart_reply(channel_name: str, recent_messages: list, current_message: str) -> str:
    context = "\n".join(recent_messages[-10:]) if recent_messages else "(no prior context)"
    return SMART_REPLY_PROMPT.format(
        channel_name=channel_name,
        context=context,
        current_message=current_message,
    )


def format_summarize(channel_name: str, messages: list) -> str:
    return SUMMARIZE_PROMPT.format(
        channel_name=channel_name,
        messages="\n".join(messages),
    )


def format_context_suggestion(channel_name: str, current_user: str, recent_messages: list) -> str:
    context = "\n".join(recent_messages[-15:])
    return CONTEXT_SUGGESTION_PROMPT.format(
        channel_name=channel_name,
        current_user=current_user,
        context=context,
    )
