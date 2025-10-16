import os
from openai import OpenAI

def get_openai_client() -> OpenAI:
    #api_key = os.getenv("OPENAI_API_KEY")
    api_key = ""
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY não definido no ambiente.")
    return OpenAI(api_key=api_key)
