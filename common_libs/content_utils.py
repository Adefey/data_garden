import html
import re


def get_safe_string(string: str | bytes | None) -> str:
    if string is None:
        return ""
    if isinstance(string, bytes):
        string = string.decode("utf-8")
    if not string:
        return ""
    string = string.strip()
    return string


def get_clear_string(text: str | bytes | None) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="ignore")
    if not text.strip():
        return ""

    text = re.sub(r"<[^>]*>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
