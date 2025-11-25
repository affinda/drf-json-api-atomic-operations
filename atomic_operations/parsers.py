"""
Parsers
"""
from typing import Dict

from rest_framework_json_api import renderers
from rest_framework_json_api.parsers import JSONParser
from rest_framework_json_api.utils import undo_format_field_name

from atomic_operations.consts import (
    ATOMIC_CONTENT_TYPE, ATOMIC_OPERATIONS,
    OP_ADD, OP_UPDATE, OP_REMOVE, OP_INVOKE, OP_UPDATE_RELATIONSHIP,
    SUPPORTED_OPERATIONS
)
from atomic_operations.exceptions import (
    InvalidPrimaryDataType,
    JsonApiParseError,
    MissingPrimaryData,
)


class AtomicOperationParser(JSONParser):
    """
    Similar to `JSONRenderer`, the `JSONParser` you may override the following methods if you
    need highly custom parsing control.

    A JSON:API client will send a payload that looks like this:

    .. code:: json

        {
            "atomic:operations": [{
                    "op": "add",
                    "data": {
                        "type": "articles",
                        "attributes": {
                            "title": "JSON API paints my bikeshed!"
                        }
                    }
                }]
        }

    We extract the attributes so that DRF serializers can work as normal.
    """

    media_type = ATOMIC_CONTENT_TYPE
    renderer_class = renderers.JSONRenderer

    def check_resource_identifier_object(self, idx: int, resource_identifier_object: Dict, operation_code: str):
        if operation_code in ["update", "remove"]:
            resource_id = resource_identifier_object.get("id")
            resource_lid = resource_identifier_object.get("lid")

            if not (resource_id or resource_lid):
                raise JsonApiParseError(
                    id="missing-id",
                    detail="The resource identifier object must contain an `id` member or a `lid` member",
                    pointer=f"/{ATOMIC_OPERATIONS}/{idx}/{'data' if operation_code == 'update' else 'ref'}"
                )

            if resource_id and resource_lid:
                raise JsonApiParseError(
                    id="multiple-id-fields",
                    detail="Only one of `id`, `lid` may be specified",
                    pointer=f"/{ATOMIC_OPERATIONS}/{idx}/{'data' if operation_code == 'update' else 'ref'}"
                )

        if not resource_identifier_object.get("type"):
            raise JsonApiParseError(
                id="missing-type",
                detail="The resource identifier object must contain an `type` member",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/{'data' if operation_code == 'update' else 'ref'}"
            )

    def check_add_operation(self, idx, data):
        if not isinstance(data, dict):
            raise MissingPrimaryData(idx)
        self.check_resource_identifier_object(idx, data, "add")

    def check_relation_update(self, idx, operation):
        self.check_resource_identifier_object(idx, operation["ref"], "update")
        # relationship update detected
        relationship = operation["ref"].get("relationship")
        if not relationship:
            # relationship update must name the attribute
            raise JsonApiParseError(
                id="missing-relationship-naming",
                detail="relationship must be named by the `relationship` attribute",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/ref"
            )
        try:
            data = operation["data"]
            if data == None:
                # clear relation, this is valid
                return

            if not isinstance(data, (dict, list)):
                # relationship update data must be a dict (to-one) or list (to-many)
                # TODO: if we know the relation type, we could provide a more detailed error message
                raise InvalidPrimaryDataType(idx, "object or array")

            if isinstance(data, dict):
                self.check_resource_identifier_object(idx, data, "update")
            else:
                for relation in data:
                    self.check_resource_identifier_object(idx, relation, "update")

        except KeyError:
            # relationship update must provide data attribute. It could be None but it must be present.
            raise MissingPrimaryData(idx)

    def check_update_operation(self, idx, operation):
        ref = operation.get("ref")
        if ref:
            self.check_relation_update(idx, operation)
        else:
            data = operation.get("data")
            if not data:
                raise MissingPrimaryData(idx)
            elif not isinstance(data, dict):
                raise InvalidPrimaryDataType(idx, "object")
            self.check_resource_identifier_object(idx, data, operation["op"])

    def check_remove_operation(self, idx, ref):
        if not ref:
            raise JsonApiParseError(
                id="missing-ref-attribute",
                detail="`ref` must be part of remove operation",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}"
            )
        self.check_resource_identifier_object(idx, ref, "remove")

    def check_invoke_operation(self, idx, operation):
        """Check invoke operation for custom actions"""
        ref = operation.get("ref")
        if not ref:
            raise JsonApiParseError(
                id="missing-ref-attribute",
                detail="`ref` is required for invoke operation",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}"
            )

        # Validate required fields for invoke operations
        if not ref.get("href"):
            raise JsonApiParseError(
                id="missing-href-attribute",
                detail="`href` is required in ref for invoke operation",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/ref"
            )

        if not ref.get("type"):
            raise JsonApiParseError(
                id="missing-type",
                detail="`type` is required in ref for invoke operation",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/ref"
            )

    def check_operation(self, idx: int, operation: Dict):
        operation_code: str = operation.get("op")
        ref: dict = operation.get("ref")
        href: str = operation.get("href")
        data = operation.get("data")

        if not operation_code:
            raise JsonApiParseError(
                id="missing-operation-code",
                detail="Received operation does not provide an operation code",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/op"
            )

        if href and operation_code != OP_INVOKE:
            # href is only supported for invoke operations (optional by standard)
            raise JsonApiParseError(
                id="not-implemented",
                detail="Operation 'href' is only supported for invoke operations. Use 'ref' instead.",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/href"
            )

        # Validate operation based on type
        if operation_code == OP_ADD:
            self.check_add_operation(idx, data)
        elif operation_code == OP_REMOVE:
            self.check_remove_operation(idx, ref)
        elif operation_code == OP_UPDATE:
            self.check_update_operation(idx, operation)
        elif operation_code == OP_INVOKE:
            self.check_invoke_operation(idx, operation)
        else:
            raise JsonApiParseError(
                id="unknown-operation-code",
                detail=f"Unknown operation '{operation_code}'. Supported operations: {', '.join(SUPPORTED_OPERATIONS)}",
                pointer=f"/{ATOMIC_OPERATIONS}/{idx}/op"
            )

    def is_bulk_operation_type(self, resource_type):
        """Check if a resource type is a bulk operation type"""
        return resource_type and resource_type.startswith("bulk")

    def parse_id_lid_and_type(self, resource_identifier_object):
        parsed_data = {"id": resource_identifier_object.get(
            "id")} if "id" in resource_identifier_object else {}
        parsed_data["type"] = resource_identifier_object.get("type")

        if lid := resource_identifier_object.get("lid", None):
            parsed_data["lid"] = lid

        return parsed_data

    def check_root(self, result):
        if not isinstance(result, dict) or ATOMIC_OPERATIONS not in result:
            raise JsonApiParseError(
                id="missing-operation-objects",
                detail="Received document does not contain operations objects",
                pointer=f"/{ATOMIC_OPERATIONS}"
            )

        # Sanity check
        if not isinstance(result.get(ATOMIC_OPERATIONS), list):
            raise JsonApiParseError(
                id="invalid-operation-objects",
                detail="Received operation objects is not a valid JSON:API atomic operation request",
                pointer=f"/{ATOMIC_OPERATIONS}"
            )

    def parse_operation_metadata(self, resource_identifier_object: dict, metadata: dict):
        """Parse the meta object from operation data if it exists"""
        if not metadata:
            return {}
        idx = None
        if "id" in resource_identifier_object:
            idx = resource_identifier_object.get("id")
        elif "lid" in resource_identifier_object:
            idx = resource_identifier_object.get("lid")
        if not isinstance(metadata, dict):
            raise JsonApiParseError(
                id="invalid-operation-meta-object",
                detail="Received operation meta data value is not valid",
                pointer=f"{ATOMIC_OPERATIONS}/{idx}/meta",
            )
        for key, value in metadata.items():
            if key == "include" and not isinstance(value, list):
                raise JsonApiParseError(
                    id="invalid-operation-include-value",
                    detail="Received operation include value is not a list",
                    pointer=f"{ATOMIC_OPERATIONS}/{idx}/meta/include",
                )
        return {"meta": metadata}

    def parse_operation(self, resource_identifier_object, result):
        _parsed_data = self.parse_id_lid_and_type(resource_identifier_object)
        _parsed_data.update(self.parse_attributes(resource_identifier_object))
        _parsed_data.update(self.parse_relationships(resource_identifier_object))
        _parsed_data.update(self.parse_metadata(result))
        return _parsed_data

    def parse_data(self, result, parser_context):
        """
        Formats the output of calling JSONParser to match the JSON:API specification
        and returns the result.
        """
        self.check_root(result)

        # Construct the return data
        parsed_data = []
        for idx, operation in enumerate(result[ATOMIC_OPERATIONS]):

            self.check_operation(idx, operation)
            meta = operation.get("meta")

            op_code = operation["op"]

            if op_code == OP_UPDATE and operation.get("ref"):
                # Handle relationship update
                ref = operation["ref"]
                ref["relationships"] = {
                    ref.pop("relationship"): {"data": operation["data"]}
                }
                _parsed_data = self.parse_operation(ref, result)
                _parsed_data.update(self.parse_operation_metadata(ref, meta))
                operation_code = OP_UPDATE_RELATIONSHIP

            elif op_code == OP_INVOKE:
                # Handle invoke operation for custom actions
                ref = operation["ref"]
                data = operation.get("data", {})

                # Parse data if it has JSON:API structure
                if "attributes" in data or "relationships" in data:
                    parsed_invoke_data = self.parse_operation(data, result)
                else:
                    parsed_invoke_data = data

                _parsed_data = {
                    "type": ref["type"],
                    "href": ref["href"],
                    "data": parsed_invoke_data
                }
                _parsed_data.update(self.parse_operation_metadata(ref, meta))
                operation_code = OP_INVOKE

            elif op_code == OP_ADD and self.is_bulk_operation_type(operation.get("data", {}).get("type")):
                # Handle bulk operations as special add operations
                # These follow the standard JSON:API format but with special resource types
                _parsed_data = self.parse_operation(operation["data"], result)
                _parsed_data.update(self.parse_operation_metadata(operation["data"], meta))
                operation_code = OP_ADD

            else:
                # Standard operations (add, update, remove)
                data = operation.get("data", operation.get("ref"))
                _parsed_data = self.parse_operation(data, result)
                _parsed_data.update(self.parse_operation_metadata(data, meta))
                operation_code = op_code

            parsed_data.append({
                operation_code: _parsed_data
            })

        return parsed_data
