from descarteslabs.common.graft import client as graft_client

from ..primitives import Float

"e = 2.71828182845904523536028747135266249775724709369995..."
e = Float._from_graft(graft_client.keyref_graft("constants.e"))


"Floating point representation of positive infinity."
inf = Float._from_graft(graft_client.keyref_graft("constants.inf"))


"Floating point representation of Not a Number."
nan = Float._from_graft(graft_client.keyref_graft("constants.nan"))


"pi = 3.1415926535897932384626433..."
pi = Float._from_graft(graft_client.keyref_graft("constants.pi"))
