from .types.primitives import Any
from .types.identifier import identifier

# NOTE(gabe): we use Any instead of a more appropriate type (like GeoContext)
# to avoid nasty circular import complexities. These objects should not be used
# directly much; they're just dummy placeholders for a key in a graft.

# direct reference to builtin keys
geoctx = identifier("geoctx", Any)
_token = identifier("_token_", Any)
_ruster = identifier("_ruster", Any)
