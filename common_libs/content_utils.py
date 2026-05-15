def get_safe_string(string: str | bytes | None) -> str:
    if string is None:
        return ""
    if isinstance(string, bytes):
        string = string.decode("utf-8")
    if not string:
        return ""
    string = string.strip()
    return string
