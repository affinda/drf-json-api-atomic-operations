"""
Renderers
"""
import json
from typing import List, OrderedDict

from rest_framework_json_api.renderers import JSONRenderer
from rest_framework_json_api.utils import get_resource_type_from_serializer

from atomic_operations.consts import (
    ATOMIC_CONTENT_TYPE,
    ATOMIC_MEDIA_TYPE,
    ATOMIC_RESULTS,
)


class AtomicResultRenderer(JSONRenderer):
    """
    The `JSONRenderer` exposes a number of methods that you may override if you need highly
    custom rendering control.

    Render a JSON response per the JSON:API spec:

    .. code-block:: json

        {
          "atomic:results": [{
            "data": {
              "links": {
                "self": "http://example.com/blogPosts/13"
              },
              "type": "articles",
              "id": "13",
              "attributes": {
                "title": "JSON API paints my bikeshed!"
              }
            }
          }]
        }
    """

    media_type = ATOMIC_CONTENT_TYPE
    format = ATOMIC_MEDIA_TYPE
    # the current atomic operation request data being rendered
    current_operation_request_data = None

    def check_error(self, operation_result_data, accepted_media_type, renderer_context):
        # primitive check if any operation has errors while parsing
        status = operation_result_data.get("status")
        try:
            status = int(status)
            if status >= 400 and status <= 600:
                return self.render_errors([operation_result_data], accepted_media_type, renderer_context)
        except Exception:
            pass

    def extract_included(
        self, fields, resource, resource_instance, included_resources, included_cache
    ):
        """
        This method will be called by the super class (JSONRenderer) render method. The
        value of the included_resources argument is set by a rest_framework_json_api
        utility function called `get_included_resources`. The utility function checks the
        Request's query_params for the `include` param which atomic_operations does not
        use for include.

        Because we cannot override the `get_included_resources` function without doing a
        monkey patch, we override extract_included to use the include value from the
        current atomic operation request data. Then we call the original
        extract_included method with an updated included_resources value.

        In order to have access to the current atomic operation's request data we make
        this method an instance method and access self.current_operation_request_data.

        Relevant django-rest-framework-json-api files:
        https://github.com/django-json-api/django-rest-framework-json-api/blob/main/rest_framework_json_api/renderers.py#L559
        https://github.com/django-json-api/django-rest-framework-json-api/blob/main/rest_framework_json_api/utils.py#L318
        """
        op_data = self.current_operation_request_data
        if op_data:
            included_resources = op_data.get("meta", {}).get("include", [])
        return JSONRenderer.extract_included(
            fields, resource, resource_instance, included_resources, included_cache
        )

    def render(self, data: List[OrderedDict], accepted_media_type=None, renderer_context=None):
        renderer_context = renderer_context or {"view": {}}

        atomic_results = []
        for operation_result_data in data:
            has_error = self.check_error(
                operation_result_data, accepted_media_type, renderer_context)
            if has_error:
                return has_error

            # pass in the resource name
            renderer_context["view"].resource_name = get_resource_type_from_serializer(
                operation_result_data.serializer)
            # make request data accessible to extract_include
            self.current_operation_request_data = operation_result_data.serializer._kwargs.get(
                "data"
            )
            rendered_primary_data = super().render(
                operation_result_data, accepted_media_type, renderer_context)
            atomic_results.append(rendered_primary_data.decode("UTF-8"))

        atomic_results_str = f"[{','.join(atomic_results)}]"

        rendered_content = '{"' + ATOMIC_RESULTS + '":' + atomic_results_str + '}'

        return rendered_content.encode()
