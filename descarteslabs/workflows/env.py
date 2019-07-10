from .types.primitives import Any

# NOTE(gabe): we use Any instead of a more appropriate type (like GeoContext)
# to avoid nasty circular import complexities. These objects should not be used
# directly much; they're just dummy placeholders for a key in a graft.

# direct reference to builtin keys
geoctx = Any._from_graft({"returns": "geoctx"})
_token = Any._from_graft({"returns": "_token_"})
