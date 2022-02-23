import graphene
from graphene.types.generic import GenericScalar

from ...core.models import ModelWithMetadata
from ..channel import ChannelContext
from .resolvers import (
    check_private_metadata_privilege,
    resolve_metadata,
    resolve_object_with_metadata_type,
    resolve_private_metadata,
)


class MetadataItem(graphene.ObjectType):
    key = graphene.String(required=True, description="Key of a metadata item.")
    value = graphene.String(required=True, description="Value of a metadata item.")


class Metadata(GenericScalar):
    """Metadata is a map of key-value pairs, both keys and values are `String`.

    Example:

    ```
    {
        "key1": "value1",
        "key2": "value2"
    }
    ```
    """


class ObjectWithMetadata(graphene.Interface):
    private_metadata = graphene.List(
        MetadataItem,
        required=True,
        description=(
            "List of private metadata items."
            "Requires proper staff permissions to access."
        ),
    )
    private_meta = Metadata()
    metadata = graphene.List(
        MetadataItem,
        required=True,
        description=(
            "List of public metadata items. Can be accessed without permissions."
        ),
    )
    meta = Metadata()

    @staticmethod
    def resolve_metadata(root: ModelWithMetadata, _info):
        return resolve_metadata(root.metadata)

    @staticmethod
    def resolve_meta(root: ModelWithMetadata, _info):
        return root.metadata

    @staticmethod
    def resolve_private_metadata(root: ModelWithMetadata, info):
        return resolve_private_metadata(root, info)

    @staticmethod
    def resolve_private_meta(root: ModelWithMetadata, info):
        check_private_metadata_privilege(root, info)
        return root.private_metadata

    @classmethod
    def resolve_type(cls, instance: ModelWithMetadata, _info):
        if isinstance(instance, ChannelContext):
            # Return instance for types that use ChannelContext
            instance = instance.node
        item_type, _ = resolve_object_with_metadata_type(instance)
        return item_type
