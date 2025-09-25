import os
from openai import OpenAI

def get_openai_client() -> OpenAI:
    #api_key = os.getenv("OPENAI_API_KEY")
    api_key = "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"
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY não definido no ambiente.")
    return OpenAI(api_key=api_key)
