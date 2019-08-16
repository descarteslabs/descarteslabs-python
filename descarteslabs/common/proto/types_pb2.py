# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: types.proto

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
  name='types.proto',
  package='descarteslabs.workflows',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x0btypes.proto\x12\x17\x64\x65scarteslabs.workflows*\xf6\x01\n\nResultType\x12\x0b\n\x07Unknown\x10\x00\x12\t\n\x05Image\x10\x01\x12\x13\n\x0fImageCollection\x10\x02\x12\x0b\n\x07\x46\x65\x61ture\x10\x03\x12\x15\n\x11\x46\x65\x61tureCollection\x10\x04\x12\n\n\x06String\x10\x05\x12\n\n\x06Number\x10\x06\x12\x08\n\x04List\x10\x07\x12\x0e\n\nDictionary\x10\x08\x12\x07\n\x03Int\x10\t\x12\t\n\x05\x46loat\x10\n\x12\x07\n\x03Str\x10\x0b\x12\t\n\x05Tuple\x10\x0c\x12\x08\n\x04\x44ict\x10\r\x12\x0c\n\x08\x44\x61tetime\x10\x0e\x12\r\n\tTimedelta\x10\x0f\x12\x08\n\x04\x42ool\x10\x10\x12\x0c\n\x08Geometry\x10\x11\x62\x06proto3')
)

_RESULTTYPE = _descriptor.EnumDescriptor(
  name='ResultType',
  full_name='descarteslabs.workflows.ResultType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='Unknown', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Image', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ImageCollection', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Feature', index=3, number=3,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FeatureCollection', index=4, number=4,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='String', index=5, number=5,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Number', index=6, number=6,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='List', index=7, number=7,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Dictionary', index=8, number=8,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Int', index=9, number=9,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Float', index=10, number=10,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Str', index=11, number=11,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Tuple', index=12, number=12,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Dict', index=13, number=13,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Datetime', index=14, number=14,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Timedelta', index=15, number=15,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Bool', index=16, number=16,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Geometry', index=17, number=17,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=41,
  serialized_end=287,
)
_sym_db.RegisterEnumDescriptor(_RESULTTYPE)

ResultType = enum_type_wrapper.EnumTypeWrapper(_RESULTTYPE)
Unknown = 0
Image = 1
ImageCollection = 2
Feature = 3
FeatureCollection = 4
String = 5
Number = 6
List = 7
Dictionary = 8
Int = 9
Float = 10
Str = 11
Tuple = 12
Dict = 13
Datetime = 14
Timedelta = 15
Bool = 16
Geometry = 17


DESCRIPTOR.enum_types_by_name['ResultType'] = _RESULTTYPE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)


# @@protoc_insertion_point(module_scope)