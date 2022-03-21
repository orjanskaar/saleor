import pytest

from ..utils.json_truncate import JsonTruncText


@pytest.mark.parametrize(
    "text,limit,expected_size,expected_text,expected_truncated",
    [
        ("abcde", 5, 5, "abcde", False),
        ("abó", 3, 2, "ab", True),
        ("abó", 8, 8, "abó", False),
        ("abó", 12, 8, "abó", False),
        ("a\nc𐀁d", 17, 17, "a\nc𐀁d", False),
        ("a\nc𐀁d", 10, 4, "a\nc", True),
        ("a\nc𐀁d", 16, 16, "a\nc𐀁", True),
        ("abcd", 0, 0, "", True),
    ],
)
def test_json_truncate_text_to_byte_limit_ensure_ascii(
    text, limit, expected_size, expected_text, expected_truncated
):
    truncated = JsonTruncText.truncate(text, limit, ensure_ascii=True)
    assert truncated.text == expected_text
    assert truncated.byte_size == expected_size
    assert truncated.truncated == expected_truncated


@pytest.mark.parametrize(
    "text,limit,expected_size,expected_text,expected_truncated",
    [
        ("abcde", 5, 5, "abcde", False),
        ("abó", 3, 2, "ab", True),
        ("abó", 8, 4, "abó", False),
        ("abó", 12, 4, "abó", False),
        ("a\nc𐀁d", 9, 9, "a\nc𐀁d", False),
        ("a\nc𐀁d", 7, 4, "a\nc", True),
        ("a\nc𐀁d", 8, 8, "a\nc𐀁", True),
        ("a\nc𐀁d", 8, 8, "a\nc𐀁", True),
        ("ab\x1fc", 8, 8, "ab\x1f", True),
        ("ab\x1fc", 9, 9, "ab\x1fc", False),
    ],
)
def test_json_truncate_text_to_byte_limit_ensure_ascii_set_false(
    text, limit, expected_size, expected_text, expected_truncated
):
    truncated = JsonTruncText.truncate(text, limit, ensure_ascii=False)
    assert truncated.text == expected_text
    assert truncated.truncated == expected_truncated
    assert truncated.byte_size == expected_size
