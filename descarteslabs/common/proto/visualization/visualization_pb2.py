# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: descarteslabs/common/proto/visualization/visualization.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n<descarteslabs/common/proto/visualization/visualization.proto\x12\x17\x64\x65scarteslabs.workflows\"\xa2\x02\n\tVizOption\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x14\n\x05\x62\x61nds\x18\x02 \x03(\tR\x05\x62\x61nds\x12\"\n\x0c\x63heckerboard\x18\x03 \x01(\x08R\x0c\x63heckerboard\x12\x1a\n\x08\x63olormap\x18\x04 \x01(\tR\x08\x63olormap\x12\x1c\n\treduction\x18\x05 \x01(\tR\treduction\x12\x41\n\x06scales\x18\x06 \x03(\x0b\x32).descarteslabs.workflows.VizOption.ScalesR\x06scales\x12 \n\x0b\x64\x65scription\x18\x07 \x01(\tR\x0b\x64\x65scription\x1a,\n\x06Scales\x12\x10\n\x03min\x18\x01 \x01(\x01R\x03min\x12\x10\n\x03max\x18\x02 \x01(\x01R\x03maxb\x06proto3')



_VIZOPTION = DESCRIPTOR.message_types_by_name['VizOption']
_VIZOPTION_SCALES = _VIZOPTION.nested_types_by_name['Scales']
VizOption = _reflection.GeneratedProtocolMessageType('VizOption', (_message.Message,), {

  'Scales' : _reflection.GeneratedProtocolMessageType('Scales', (_message.Message,), {
    'DESCRIPTOR' : _VIZOPTION_SCALES,
    '__module__' : 'descarteslabs.common.proto.visualization.visualization_pb2'
    # @@protoc_insertion_point(class_scope:descarteslabs.workflows.VizOption.Scales)
    })
  ,
  'DESCRIPTOR' : _VIZOPTION,
  '__module__' : 'descarteslabs.common.proto.visualization.visualization_pb2'
  # @@protoc_insertion_point(class_scope:descarteslabs.workflows.VizOption)
  })
_sym_db.RegisterMessage(VizOption)
_sym_db.RegisterMessage(VizOption.Scales)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _VIZOPTION._serialized_start=90
  _VIZOPTION._serialized_end=380
  _VIZOPTION_SCALES._serialized_start=336
  _VIZOPTION_SCALES._serialized_end=380
# @@protoc_insertion_point(module_scope)
