import os as _os
import sys as _sys

# The generated *_pb2_grpc.py files use bare imports like
# "import DanubeApi_pb2". Adding this directory to sys.path
# ensures those imports resolve without modifying generated code.
_proto_dir = _os.path.dirname(_os.path.abspath(__file__))
if _proto_dir not in _sys.path:
    _sys.path.insert(0, _proto_dir)
