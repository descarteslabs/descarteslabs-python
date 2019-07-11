from descarteslabs.common.graft import client as graft_client

from ..primitives import Float

e = Float._from_graft(graft_client.keyref_graft("constants.e"))
"e = 2.71828182845904523536028747135266249775724709369995..."


inf = Float._from_graft(graft_client.keyref_graft("constants.inf"))
"Floating point representation of positive infinity."


nan = Float._from_graft(graft_client.keyref_graft("constants.nan"))
"Floating point representation of Not a Number."


pi = Float._from_graft(graft_client.keyref_graft("constants.pi"))
"pi = 3.1415926535897932384626433..."
