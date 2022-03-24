import json
from datetime import datetime

import pytest
import pytz
from celery.exceptions import Retry
from freezegun import freeze_time

from ..utils import (
    CustomJsonEncoder,
    JsonTruncText,
    hide_sensitive_headers,
    task_next_retry_date,
)


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


@pytest.mark.parametrize(
    "retry, next_retry_date",
    [
        (Retry(), None),
        (Retry(when=60 * 10), datetime(1914, 6, 28, 11, tzinfo=pytz.utc)),
        (Retry(when=datetime(1914, 6, 28, 11)), datetime(1914, 6, 28, 11)),
    ],
)
@freeze_time("1914-06-28 10:50")
def test_task_next_retry_date(retry, next_retry_date):
    assert task_next_retry_date(retry) == next_retry_date


@pytest.mark.parametrize(
    "headers,sensitive,expected",
    [
        (
            {"header1": "text", "header2": "text"},
            ("AUTHORIZATION", "AUTHORIZATION_BEARER"),
            {"header1": "text", "header2": "text"},
        ),
        (
            {"header1": "text", "authorization": "secret"},
            ("AUTHORIZATION", "AUTHORIZATION_BEARER"),
            {"header1": "text", "authorization": "***"},
        ),
        (
            {"HEADER1": "text", "authorization-bearer": "secret"},
            ("AUTHORIZATION", "AUTHORIZATION_BEARER"),
            {"HEADER1": "text", "authorization-bearer": "***"},
        ),
    ],
)
def test_hide_sensitive_headers(headers, sensitive, expected):
    assert hide_sensitive_headers(headers, sensitive_headers=sensitive) == expected


def test_custom_json_encoder_dumps_json_trunc_text():
    # given
    input = {"body": JsonTruncText("content", truncated=True)}

    # wehen
    serialized_data = json.dumps(input, cls=CustomJsonEncoder)

    # then
    data = json.loads(serialized_data)
    assert data["body"]["text"] == "content"
    assert data["body"]["truncated"] is True
