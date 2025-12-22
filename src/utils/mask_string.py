def mask_string(value: str, visible_chars: int = 3) -> str:
    if not value or len(value) <= visible_chars:
        return "***"
    return f"{value[:visible_chars]}***"