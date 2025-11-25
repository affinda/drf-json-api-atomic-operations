"""
Serializers
"""
import inflection

from rest_framework.exceptions import ParseError


class AtomicOperationIncludedResourcesValidationMixin:
    """
    Heavily based on django-restframework-json-api IncludedResourcesValidationMixin.
    A serializer mixin that adds validation of `include` data to
    support compound documents.

    Specification: https://jsonapi.org/format/#document-compound-documents)
    """

    def __init__(self, *args, **kwargs):
        request_data = kwargs.get("data")
        context = kwargs.get("context")
        request = context.get("request") if context else None
        view = context.get("view") if context else None
        operation_code = context.get("operation_code") if context else None
        resource_type = context.get("resource_type") if context else None

        def validate_path(serializer_class, field_path, path):
            serializers = getattr(serializer_class, "included_serializers", None)
            if serializers is None:
                raise ParseError("This endpoint does not support the include parameter")
            this_field_name = inflection.underscore(field_path[0])
            this_included_serializer = serializers.get(this_field_name)
            if this_included_serializer is None:
                raise ParseError(
                    "This endpoint does not support the include parameter for path {}".format(path)
                )
            if len(field_path) > 1:
                new_included_field_path = field_path[1:]
                # We go down one level in the path
                validate_path(this_included_serializer, new_included_field_path, path)

        if request and view:
            meta_data = request_data.get("meta", {}) if request_data else {}
            included_resources = meta_data.get("include", [])
            for included_field_name in included_resources:
                included_field_path = included_field_name.split(".")
                if "related_field" in view.kwargs:
                    this_serializer_class = view.get_related_serializer_class()
                else:
                    this_serializer_class = view.get_serializer_class(operation_code, resource_type)
                # lets validate the current path
                validate_path(this_serializer_class, included_field_path, included_field_name)

        super().__init__(*args, **kwargs)
