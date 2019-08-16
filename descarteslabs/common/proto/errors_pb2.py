# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: errors.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='errors.proto',
  package='descarteslabs.workflows',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x0c\x65rrors.proto\x12\x17\x64\x65scarteslabs.workflows*\x9f\x01\n\tErrorCode\x12\x0e\n\nERROR_NONE\x10\x00\x12\x11\n\rERROR_UNKNOWN\x10\x01\x12\x11\n\rERROR_INVALID\x10\x02\x12\x12\n\x0e\x45RROR_DEADLINE\x10\x03\x12\r\n\tERROR_OOM\x10\t\x12\x13\n\x0f\x45RROR_INTERRUPT\x10\x0f\x12\x14\n\x10\x45RROR_TERMINATED\x10\x10\x12\x0e\n\nERROR_AUTH\x10\x11\x62\x06proto3')
)

_ERRORCODE = _descriptor.EnumDescriptor(
  name='ErrorCode',
  full_name='descarteslabs.workflows.ErrorCode',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='ERROR_NONE', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_UNKNOWN', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_INVALID', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_DEADLINE', index=3, number=3,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_OOM', index=4, number=9,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_INTERRUPT', index=5, number=15,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_TERMINATED', index=6, number=16,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR_AUTH', index=7, number=17,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=42,
  serialized_end=201,
)
_sym_db.RegisterEnumDescriptor(_ERRORCODE)

ErrorCode = enum_type_wrapper.EnumTypeWrapper(_ERRORCODE)
ERROR_NONE = 0
ERROR_UNKNOWN = 1
ERROR_INVALID = 2
ERROR_DEADLINE = 3
ERROR_OOM = 9
ERROR_INTERRUPT = 15
ERROR_TERMINATED = 16
ERROR_AUTH = 17


DESCRIPTOR.enum_types_by_name['ErrorCode'] = _ERRORCODE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)


# @@protoc_insertion_point(module_scope)