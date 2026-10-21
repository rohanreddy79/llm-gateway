"""Point the official OpenAI Python SDK at llm-gateway.

    uv run --with openai examples/openai_sdk.py

Requires a running gateway (`llm-gateway serve --config gateway.yaml`).
"""

from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:19080/v1", api_key="gw-demo-key")

resp = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Hello"}],
    temperature=0,
)
print(resp.model, resp.usage.total_tokens, "tokens")
print(resp.choices[0].message.content)

stream = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Hello"}],
    stream=True,
)
for chunk in stream:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
    if chunk.usage:
        print(f"\n[usage: {chunk.usage.completion_tokens} completion tokens]")
