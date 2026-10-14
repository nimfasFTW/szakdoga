"""The debugging agent: a LangChain tool-calling agent on Gemini.

This module only knows how to run the model against a list of tools. What the tools do is in
tools.py, and whether the cluster ended up healthy is decided by resolver.py, never by what the
agent claims in its final message.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Sequence

from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.errors import GraphRecursionError

logger = logging.getLogger("llm_resolver.agent")

MODEL_NAME = os.getenv("LLM_MODEL", "gemini-3.7-flash")

SYSTEM_PROMPT = """\
You are an autonomous Kubernetes SRE agent. A monitoring alert fired for a workload. Find the \
root cause and fix it, using only the tools you are given. The cluster is live: every change \
you make is applied immediately.

How to work:
1. Investigate before you change anything. Look at the affected workload's pods (state, restarts, \
exit codes), its events, its logs (use previous=True for a container that restarted) and its \
configuration.
2. The root cause is not always in the workload that alerted. A workload can be unhealthy because \
something it depends on (another Deployment, a Service, a ConfigMap) is broken or misconfigured. \
When logs or configuration point at a dependency, inspect that too.
3. Form a hypothesis from the evidence and make the smallest change that fixes the root cause. \
Fix configuration at its source rather than working around a symptom. Do not change things that \
are unrelated to the problem, and do not change several things when one is enough.
4. After every change, call check_health on the workload that alerted to confirm it is really \
healthy. If it is not, look at the new evidence and try something else.
5. Stop when the workload is healthy, or when you conclude that you cannot fix it with your tools \
(for example the cause lies outside the cluster). Do not make speculative changes to a problem \
you cannot fix; explain it instead.

Things to know:
- Environment variables that come from a ConfigMap are read only when a container starts. After \
changing a ConfigMap you must restart the Deployments that use it.
- Changing a Deployment's pod template (image, env, resources) starts a new rollout by itself.
- You cannot see container registries. Only set an image tag you are confident exists.
- A Service routes by its selector and forwards to targetPort; a client that uses the wrong \
port, or a Service with the wrong targetPort, fails even though all pods are running.

When you are done, reply with a short plain-text report in exactly this form:
ROOT CAUSE: <one or two sentences>
ACTIONS: <what you changed, or "none">
OUTCOME: <healthy | not fixed, and why>
"""


@dataclass
class AgentRun:
    summary: str                     # the agent's final message (its own report; not trusted as a verdict)
    steps: int                       # model calls
    input_tokens: int
    output_tokens: int
    model: str
    error: Optional[str] = None      # set when the run ended abnormally; the tool record is still valid


@lru_cache(maxsize=1)
def get_model() -> ChatGoogleGenerativeAI:   # lazy: importing this module must not need credentials
    if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
        logger.warning("GOOGLE_API_KEY is not set; the first model call will fail")
    return ChatGoogleGenerativeAI(
        model=MODEL_NAME,
        temperature=float(os.getenv("LLM_TEMPERATURE", "1.0")),   # Gemini 3+ is tuned for 1.0
        max_tokens=None,
        timeout=float(os.getenv("LLM_TIMEOUT_S", "90")),          # one hung request must not hang the incident
        max_retries=2,
    )


def _usage(messages: Sequence) -> tuple[int, int, int]:
    steps = tokens_in = tokens_out = 0
    for m in messages:
        if isinstance(m, AIMessage):
            steps += 1
            u = m.usage_metadata or {}
            tokens_in += u.get("input_tokens", 0)
            tokens_out += u.get("output_tokens", 0)
    return steps, tokens_in, tokens_out


def investigate(prompt: str, tools: Sequence, max_steps: int) -> AgentRun:
    """Run the agent until it stops calling tools or `max_steps` model calls are used.

    Runtime failures (rate limit, step limit, ...) do not raise: whatever the agent did up to
    that point is still in the toolkit's record, and the resolver must verify the cluster anyway.
    Only a failure to even set up the model raises."""
    agent = create_agent(get_model(), list(tools), system_prompt=SYSTEM_PROMPT)

    messages: list = []
    error: Optional[str] = None
    try:
        # stream_mode="values" yields the full state after every step, so if the run dies half way
        # we still have the conversation so far
        for state in agent.stream({"messages": [{"role": "user", "content": prompt}]},
                                  config={"recursion_limit": 2 * max_steps},   # a step = model node + tools node, so this allows max_steps model calls
                                  stream_mode="values"):
            messages = state["messages"]
    except GraphRecursionError:
        error = "step_limit_reached"
        logger.warning("agent hit the step limit (%d model calls)", max_steps)
    except Exception as e:
        error = repr(e)
        logger.exception("agent run failed")

    steps, tokens_in, tokens_out = _usage(messages)
    last = messages[-1] if messages else None
    summary = last.text if isinstance(last, AIMessage) else ""
    return AgentRun(summary=summary.strip(), steps=steps, input_tokens=tokens_in,
                    output_tokens=tokens_out, model=MODEL_NAME, error=error)
