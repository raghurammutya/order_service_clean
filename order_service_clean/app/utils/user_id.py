"""
User ID Extraction Utilities

Provides safe parsing of user IDs from various formats.

Author: Claude Code
Date: 2025-11-25
"""


def extract_user_id(current_user: dict) -> int:
    """
    Safely extract numeric user ID from user context.

    Handles formats:
    - "user:123" -> 123
    - "prefix:value:123" -> 123 (takes last numeric segment)
    - "123" -> 123
    - 123 -> 123

    Args:
        current_user: User context dictionary with 'user_id' key

    Returns:
        Integer user ID

    Raises:
        ValueError: If user_id cannot be parsed
    """
    user_id_raw = current_user.get("user_id")
    if user_id_raw is None:
        raise ValueError("No user_id in current_user context")

    if isinstance(user_id_raw, int):
        return user_id_raw

    user_id_str = str(user_id_raw)

    if ":" in user_id_str:
        # Format: "user:123" or "prefix:123" or "a:b:123"
        parts = user_id_str.split(":")
        # Find the last numeric part
        for part in reversed(parts):
            if part.isdigit():
                return int(part)
        raise ValueError(f"Cannot parse user_id from format: {user_id_str}")

    if user_id_str.isdigit():
        return int(user_id_str)

    raise ValueError(f"Invalid user_id format: {user_id_str}")
