import pytest

from order_service.app.utils.user_id import extract_user_id


def test_extract_user_id_from_int_and_string():
    assert extract_user_id({"user_id": 5}) == 5
    assert extract_user_id({"user_id": "123"}) == 123


def test_extract_user_id_from_colon_formats():
    assert extract_user_id({"user_id": "user:123"}) == 123
    assert extract_user_id({"user_id": "a:b:789"}) == 789


def test_extract_user_id_invalid_inputs():
    with pytest.raises(ValueError):
        extract_user_id({"user_id": "abc"})
    with pytest.raises(ValueError):
        extract_user_id({"user_id": None})
    with pytest.raises(ValueError):
        extract_user_id({})
