# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: descarteslabs/common/proto/logging/logging.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n0descarteslabs/common/proto/logging/logging.proto\x12\x17\x64\x65scarteslabs.workflows\"\xb9\x01\n\tLogRecord\x12>\n\x05level\x18\x01 \x01(\x0e\x32(.descarteslabs.workflows.LogRecord.LevelR\x05level\x12\x18\n\x07message\x18\x03 \x01(\tR\x07message\x12\x1c\n\ttimestamp\x18\x04 \x01(\x03R\ttimestamp\"4\n\x05Level\x12\t\n\x05\x44\x45\x42UG\x10\x00\x12\x08\n\x04INFO\x10\x01\x12\x0b\n\x07WARNING\x10\x02\x12\t\n\x05\x45RROR\x10\x03\x62\x06proto3')



_LOGRECORD = DESCRIPTOR.message_types_by_name['LogRecord']
_LOGRECORD_LEVEL = _LOGRECORD.enum_types_by_name['Level']
LogRecord = _reflection.GeneratedProtocolMessageType('LogRecord', (_message.Message,), {
  'DESCRIPTOR' : _LOGRECORD,
  '__module__' : 'descarteslabs._dl_modules.common.proto.logging.logging_pb2'
  # @@protoc_insertion_point(class_scope:descarteslabs.workflows.LogRecord)
  })
_sym_db.RegisterMessage(LogRecord)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _LOGRECORD._serialized_start=78
  _LOGRECORD._serialized_end=263
  _LOGRECORD_LEVEL._serialized_start=211
  _LOGRECORD_LEVEL._serialized_end=263
# @@protoc_insertion_point(module_scope)