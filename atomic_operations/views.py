from typing import Dict, List
from collections import defaultdict

from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist
from django.db.transaction import atomic
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from atomic_operations.consts import (
    ATOMIC_OPERATIONS,
    OP_ADD, OP_UPDATE, OP_REMOVE, OP_INVOKE, OP_UPDATE_RELATIONSHIP
)
from atomic_operations.exceptions import UnprocessableEntity
from atomic_operations.parsers import AtomicOperationParser
from atomic_operations.renderers import AtomicResultRenderer


class AtomicOperationView(APIView):
    """View which handles JSON:API Atomic Operations extension https://jsonapi.org/ext/atomic/"""

    renderer_classes = [AtomicResultRenderer]
    parser_classes = [AtomicOperationParser]

    # only post method is allowed https://jsonapi.org/ext/atomic/#processing
    http_method_names = ["post"]

    #
    serializer_classes: Dict = {}

    sequential = True
    response_data: List[Dict] = []

    lid_to_id = defaultdict(dict)

    # TODO: proof how to check permissions for all operations
    # permission_classes = TODO
    # call def check_permissions for `add` operation
    # call def check_object_permissions for `update` and `remove` operation

    def get_serializer_classes(self) -> Dict:
        if self.serializer_classes:
            return self.serializer_classes
        else:
            raise ImproperlyConfigured("You need to define the serializer classes. "
                                       "Otherwise serialization of json:api primary data is not possible.")

    def extract_action_from_href(self, href: str) -> str:
        """Extract the action name from an href path.
        
        Examples:
            '/api/annotations/create' -> 'create'
            '/api/annotations/delete/' -> 'delete'
        """
        return href.rstrip('/').split('/')[-1]

    def is_bulk_operation_type(self, resource_type: str) -> bool:
        """Check if a resource type is a bulk operation type"""
        return resource_type and resource_type.startswith("bulk")
    
    def get_serializer_class(self, operation_code: str, resource_type: str, href: str = None):
        if operation_code == OP_INVOKE and href:
            # For invoke operations, use the href to determine the serializer
            action = self.extract_action_from_href(href)
            key = f"{operation_code}:{resource_type}/{action}"
        elif operation_code == OP_ADD and self.is_bulk_operation_type(resource_type):
            # For bulk operations (e.g., bulkAnnotationCreate), use the resource type directly
            key = f"{operation_code}:{resource_type}"
        else:
            key = f"{operation_code}:{resource_type}"
            
        serializer_class = self.get_serializer_classes().get(key)
        if serializer_class:
            return serializer_class
        else:
            raise ImproperlyConfigured(
                f"No serializer found for resource type '{resource_type}' and operation '{operation_code}' (key: {key})")

    def get_serializer(self, idx, operation_code, resource_type, href=None, *args, **kwargs):
        """
        Return the serializer instance that should be used for validating and
        deserializing input, and for serializing output.
        """
        serializer_class = self.get_serializer_class(
            operation_code, resource_type, href)
        kwargs.setdefault('context', self.get_serializer_context())

        if operation_code in [OP_UPDATE, OP_REMOVE]:
            try:
                kwargs.update({
                    "instance": serializer_class.Meta.model.objects.get(pk=kwargs["data"]["id"])
                })
            except ObjectDoesNotExist:
                raise UnprocessableEntity([
                    {
                        "id": "object-does-not-exist",
                        "detail": f'Object with id `{kwargs["data"]["id"]}` received for operation with index `{idx}` does not exist',
                        "source": {
                            "pointer": f"/{ATOMIC_OPERATIONS}/{idx}/data/id"
                        },
                        "status": "422"
                    }
                ]
                )
        # For invoke operations, no special handling needed here
        # The serializer will handle the specific format

        return serializer_class(*args, **kwargs)

    def get_serializer_context(self):
        """
        Extra context provided to the serializer class.
        """
        return {
            'request': self.request,
            'format': self.format_kwarg,
            'view': self

        }

    def post(self, request, *args, **kwargs):
        return self.perform_operations(request.data)

    def handle_sequential(self, serializer, operation_code):
        if operation_code in [OP_ADD, OP_UPDATE, OP_UPDATE_RELATIONSHIP]:
            lid = serializer.initial_data.get("lid", None)
            resource_type = serializer.initial_data.get("type")
            
            # Check if this is a bulk operation type
            if operation_code == OP_ADD and self.is_bulk_operation_type(resource_type):
                # Handle bulk operations specially
                serializer.is_valid(raise_exception=True)
                result = serializer.save()
                # For bulk operations, the result might be a special structure
                if hasattr(result, 'data'):
                    self.response_data.append(result.data)
                else:
                    self.response_data.append(serializer.data)
            else:
                serializer.is_valid(raise_exception=True)
                serializer.save()

                if operation_code == OP_ADD and lid:
                    self.lid_to_id[resource_type][lid] = serializer.data["id"]

                if operation_code != OP_UPDATE_RELATIONSHIP:
                    self.response_data.append(serializer.data)
        elif operation_code == OP_REMOVE:
            serializer.instance.delete()
        elif operation_code == OP_INVOKE:
            # Handle invoke operation - validate and execute custom action
            serializer.is_valid(raise_exception=True)
            serializer.save()
            self.response_data.append(serializer.data)

    def perform_bulk_create(self, bulk_operation_data):
        objs = []
        model_class = bulk_operation_data["serializer_collection"][0].Meta.model
        for _serializer in bulk_operation_data["serializer_collection"]:
            _serializer.is_valid(raise_exception=True)
            instance = model_class(**_serializer.validated_data)
            objs.append(instance)
        model_class.objects.bulk_create(
            objs)
        # append serialized data after save has successfully called. Otherwise id could be None. See #3
        self.response_data.extend(
            [_serializer.__class__(instance=obj).data for obj in objs])

    def perform_bulk_delete(self, bulk_operation_data):
        obj_ids = []
        for _serializer in bulk_operation_data["serializer_collection"]:
            obj_ids.append(_serializer.instance.pk)
            self.response_data.append(_serializer.data)
        bulk_operation_data["serializer_collection"][0].Meta.model.objects.filter(
            pk__in=obj_ids).delete()

    def handle_bulk(self, serializer, current_operation_code, bulk_operation_data):
        bulk_operation_data["serializer_collection"].append(serializer)
        if bulk_operation_data["next_operation_code"] != current_operation_code or bulk_operation_data["next_resource_type"] != serializer.initial_data["type"]:
            if current_operation_code == "add":
                self.perform_bulk_create(bulk_operation_data)
            elif current_operation_code == "delete":
                self.perform_bulk_delete(bulk_operation_data)
            else:
                # TODO: update in bulk requires more logic cause it could be a partial update and every field differs pers instance.
                # Then we can't do a bulk operation. This is only possible for instances which changes the same field(s).
                # Maybe the anylsis of this takes longer than simple handling updates in sequential mode.
                # For now we handle updates always in sequential mode
                self.handle_sequential(
                    bulk_operation_data["serializer_collection"][0], current_operation_code)
            bulk_operation_data["serializer_collection"] = []

    def substitute_lids(self, data, idx, should_raise_unknown_lid_error):
        if not isinstance(data, dict):
            return

        try:
            lid = data.get("lid", None)
            if lid:
                resource_type = data["type"]
                data["id"] = self.lid_to_id[resource_type][lid]
        except KeyError:
            if should_raise_unknown_lid_error:
                raise UnprocessableEntity([
                    {
                        "id": "unknown-lid",
                        "detail": f'Object with lid `{lid}` received for operation with index `{idx}` does not exist',
                        "source": {
                            "pointer": f"/{ATOMIC_OPERATIONS}/{idx}/data/lid"
                        },
                        "status": "422"
                    }
                ])
            
        for _, value in data.items():
            if isinstance(value, dict):
                self.substitute_lids(value, idx, should_raise_unknown_lid_error=True)
            elif isinstance(value, list):
                [self.substitute_lids(value, idx, should_raise_unknown_lid_error=True) for value in value]

        return data     
        
    def perform_operations(self, parsed_operations: List[Dict]):
        self.response_data = []  # reset local response data storage

        bulk_operation_data = {
            "serializer_collection": [],
            "next_operation_code": "",
            "next_resource_type": ""
        }

        with atomic():

            for idx, operation in enumerate(parsed_operations):
                operation_code = next(iter(operation))
                obj = operation[operation_code]

                should_raise_unknown_lid_error = operation_code != OP_ADD
                self.substitute_lids(obj, idx, should_raise_unknown_lid_error)

                # Extract href for invoke operations
                href = obj.get("href") if operation_code == OP_INVOKE else None
                
                # Prepare data and operation code for serializer
                if operation_code == OP_INVOKE:
                    serializer_data = obj.get("data", obj)
                else:
                    serializer_data = obj
                    
                effective_operation_code = OP_UPDATE if operation_code == OP_UPDATE_RELATIONSHIP else operation_code
                
                serializer = self.get_serializer(
                    idx=idx,
                    data=serializer_data,
                    operation_code=effective_operation_code,
                    resource_type=obj["type"],
                    href=href,
                    partial=operation_code in [OP_UPDATE, OP_UPDATE_RELATIONSHIP]
                )

                if self.sequential:
                    self.handle_sequential(serializer, operation_code)
                else:
                    is_last_iter = parsed_operations.__len__() == idx + 1
                    if is_last_iter:
                        bulk_operation_data["next_operation_code"] = ""
                        bulk_operation_data["next_resource_type"] = ""
                    else:
                        next_operation = parsed_operations[idx + 1]
                        bulk_operation_data["next_operation_code"] = next(
                            iter(next_operation))
                        bulk_operation_data["next_resource_type"] = next_operation[bulk_operation_data["next_operation_code"]]["type"]

                    self.handle_bulk(
                        serializer=serializer,
                        current_operation_code=operation_code,
                        bulk_operation_data=bulk_operation_data
                    )

        return Response(self.response_data, status=status.HTTP_200_OK if self.response_data else status.HTTP_204_NO_CONTENT)
