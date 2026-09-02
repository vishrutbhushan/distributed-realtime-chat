"""
Prompt templates for LLM features.

These templates are ready to be used with any instruct-tuned LLM.
The inference.py module formats these templates and passes them to the model.

LLM model configuration is commented out in inference.py.
Uncomment and configure when a specific model is chosen.
"""


SMART_REPLY_PROMPT = """\
You are a helpful chat assistant.

Recent conversation in #{channel_name}:
{context}

The last message is:
{current_message}

Generate exactly 3 short, natural reply suggestions (one per line).
Do not number them. Keep each reply under 15 words.
Replies:"""


SUMMARIZE_PROMPT = """\
You are a helpful assistant that summarises chat conversations.

Channel: #{channel_name}

Messages:
{messages}

Write a concise bullet-point summary of the key topics, decisions, and action items.
Summary:"""


CONTEXT_SUGGESTION_PROMPT = """\
You are a helpful assistant monitoring a team chat channel.

Channel: #{channel_name}
Current user: {current_user}

Recent messages:
{context}

Based on the conversation, suggest one concrete, actionable next step for {current_user}.
Keep it to one or two sentences.
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
