from dataclasses import asdict
from operator import itemgetter
from unittest.mock import ANY

import graphene
import pytest

from ...attribute.models import AttributeValue
from ...attribute.utils import associate_attribute_values_to_instance
from ..serializers import JsonTruncText, serialize_product_or_variant_attributes


def test_serialize_product_attributes(
    product_with_variant_with_two_attributes,
    product_with_multiple_values_attributes,
    product_type_page_reference_attribute,
    page,
):
    variant_data = serialize_product_or_variant_attributes(
        product_with_variant_with_two_attributes.variants.first()
    )

    product_type = product_with_multiple_values_attributes.product_type
    product_type.product_attributes.add(product_type_page_reference_attribute)
    attr_value = AttributeValue.objects.create(
        attribute=product_type_page_reference_attribute,
        name=page.title,
        slug=f"{product_with_multiple_values_attributes.pk}_{page.pk}",
        reference_page=page,
    )
    associate_attribute_values_to_instance(
        product_with_multiple_values_attributes,
        product_type_page_reference_attribute,
        attr_value,
    )
    product_data = serialize_product_or_variant_attributes(
        product_with_multiple_values_attributes
    )
    assert len(variant_data) == 2
    assert variant_data[1] == {
        "entity_type": None,
        "id": ANY,
        "input_type": "dropdown",
        "name": "Size",
        "slug": "size",
        "unit": None,
        "values": [
            {
                "file": None,
                "name": "Small",
                "reference": None,
                "rich_text": None,
                "date_time": None,
                "date": None,
                "boolean": None,
                "slug": "small",
                "value": "",
            }
        ],
    }

    assert len(product_data) == 2
    assert product_data[0]["name"] == "Available Modes"
    assert sorted(product_data[0]["values"], key=itemgetter("name")) == [
        {
            "name": "Eco Mode",
            "slug": "eco",
            "file": None,
            "reference": None,
            "rich_text": None,
            "date_time": None,
            "date": None,
            "boolean": None,
            "value": "",
        },
        {
            "name": "Performance Mode",
            "slug": "power",
            "file": None,
            "reference": None,
            "rich_text": None,
            "date_time": None,
            "date": None,
            "boolean": None,
            "value": "",
        },
    ]
    assert product_data[1]["name"] == "Page reference"
    assert sorted(product_data[1]["values"], key=itemgetter("name")) == [
        {
            "name": "Test page",
            "slug": attr_value.slug,
            "file": None,
            "reference": graphene.Node.to_global_id(
                attr_value.attribute.entity_type, page.pk
            ),
            "rich_text": None,
            "date_time": None,
            "date": None,
            "boolean": None,
            "value": "",
        },
    ]


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


def test_json_truncate_text_asdict():
    text = "abcde"
    truncated = JsonTruncText.truncate(text, 20)
    assert asdict(truncated) == {"text": text, "truncated": False}
