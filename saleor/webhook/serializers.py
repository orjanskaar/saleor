from dataclasses import InitVar, dataclass
from datetime import date, datetime
from json.encoder import ESCAPE_ASCII, ESCAPE_DCT  # type: ignore
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

import graphene

from ..attribute import AttributeEntityType, AttributeInputType
from ..checkout.fetch import fetch_checkout_lines
from ..core.prices import quantize_price
from ..discount import DiscountInfo
from ..product.models import Product

if TYPE_CHECKING:
    # pylint: disable=unused-import
    from ..checkout.models import Checkout
    from ..product.models import ProductVariant


def serialize_checkout_lines(
    checkout: "Checkout", discounts: Optional[Iterable[DiscountInfo]] = None
) -> List[dict]:
    data = []
    channel = checkout.channel
    currency = channel.currency_code
    lines, _ = fetch_checkout_lines(checkout, prefetch_variant_attributes=True)
    for line_info in lines:
        variant = line_info.variant
        channel_listing = line_info.channel_listing
        collections = line_info.collections
        product = variant.product
        base_price = variant.get_price(
            product, collections, channel, channel_listing, discounts or []
        )
        data.append(
            {
                "sku": variant.sku,
                "variant_id": variant.get_global_id(),
                "quantity": line_info.line.quantity,
                "base_price": str(quantize_price(base_price.amount, currency)),
                "currency": currency,
                "full_name": variant.display_product(),
                "product_name": product.name,
                "variant_name": variant.name,
                "attributes": serialize_product_or_variant_attributes(variant),
            }
        )
    return data


def serialize_product_or_variant_attributes(
    product_or_variant: Union["Product", "ProductVariant"]
) -> List[Dict]:
    data = []

    def _prepare_reference(attribute, attr_value):
        if attribute.input_type != AttributeInputType.REFERENCE:
            return
        if attribute.entity_type == AttributeEntityType.PAGE:
            reference_pk = attr_value.reference_page_id
        elif attribute.entity_type == AttributeEntityType.PRODUCT:
            reference_pk = attr_value.reference_product_id
        else:
            return None

        reference_id = graphene.Node.to_global_id(attribute.entity_type, reference_pk)
        return reference_id

    for attr in product_or_variant.attributes.all():
        attr_id = graphene.Node.to_global_id("Attribute", attr.assignment.attribute_id)
        attribute = attr.assignment.attribute
        attr_data: Dict[Any, Any] = {
            "name": attribute.name,
            "input_type": attribute.input_type,
            "slug": attribute.slug,
            "entity_type": attribute.entity_type,
            "unit": attribute.unit,
            "id": attr_id,
            "values": [],
        }

        for attr_value in attr.values.all():
            attr_slug = attr_value.slug
            value: Dict[
                str, Optional[Union[str, datetime, date, bool, Dict[str, Any]]]
            ] = {
                "name": attr_value.name,
                "slug": attr_slug,
                "value": attr_value.value,
                "rich_text": attr_value.rich_text,
                "boolean": attr_value.boolean,
                "date_time": attr_value.date_time,
                "date": attr_value.date_time,
                "reference": _prepare_reference(attribute, attr_value),
                "file": None,
            }

            if attr_value.file_url:
                value["file"] = {
                    "content_type": attr_value.content_type,
                    "file_url": attr_value.file_url,
                }
            attr_data["values"].append(value)  # type: ignore

        data.append(attr_data)

    return data


@dataclass
class JsonTruncText:
    text: str = ""
    truncated: bool = False
    added_bytes: InitVar[int] = 0
    ensure_ascii: InitVar[bool] = True

    def __post_init__(self, added_bytes, ensure_ascii):
        self._added_bytes = added_bytes
        self._ensure_ascii = ensure_ascii

    @property
    def byte_size(self) -> int:
        return len(self.text) + self._added_bytes

    @staticmethod
    def json_char_len(char: str, ensure_ascii=True) -> int:
        try:
            return len(ESCAPE_DCT[char])
        except KeyError:
            if ensure_ascii:
                return 6 if ord(char) < 0x10000 else 12
            return len(char.encode())

    @classmethod
    def truncate(cls, s: str, limit: int, ensure_ascii=True):
        limit = max(limit, 0)
        s_init_len = len(s)
        s = s[:limit]
        added_bytes = 0

        for match in ESCAPE_ASCII.finditer(s):
            start, end = match.span(0)
            markup = cls.json_char_len(match.group(0), ensure_ascii) - 1
            added_bytes += markup
            if end + added_bytes > limit:
                return cls(
                    text=s[:start],
                    truncated=True,
                    added_bytes=added_bytes - markup,
                    ensure_ascii=ensure_ascii,
                )
            elif end + added_bytes == limit:
                s = s[:end]
                return cls(
                    text=s,
                    truncated=len(s) < s_init_len,
                    added_bytes=added_bytes,
                    ensure_ascii=ensure_ascii,
                )
        return cls(
            text=s,
            truncated=len(s) < s_init_len,
            added_bytes=added_bytes,
            ensure_ascii=ensure_ascii,
        )
