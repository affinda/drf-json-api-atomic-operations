ATOMIC_OPERATIONS = "atomic:operations"
ATOMIC_RESULTS = "atomic:results"
ATOMIC_MEDIA_TYPE = 'vnd.api+json;ext="https://jsonapi.org/ext/atomic"'
ATOMIC_CONTENT_TYPE = f'application/{ATOMIC_MEDIA_TYPE}'

# Operation codes
OP_ADD = "add"
OP_UPDATE = "update"
OP_REMOVE = "remove"
OP_INVOKE = "invoke"
OP_UPDATE_RELATIONSHIP = "update-relationship"

# Supported operations
SUPPORTED_OPERATIONS = {OP_ADD, OP_UPDATE, OP_REMOVE, OP_INVOKE}
