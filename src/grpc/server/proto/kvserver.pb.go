// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.9
// source: kvserver.proto

package kvserver

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ErrCode int32

const (
	ErrCode_WRONG_LEADER    ErrCode = 0
	ErrCode_OK              ErrCode = 1
	ErrCode_NO_KEY          ErrCode = 2
	ErrCode_INVALID_SESSION ErrCode = 3
	ErrCode_INVALID_OP      ErrCode = 4
	ErrCode_INVALID_TOKEN   ErrCode = 5
)

// Enum value maps for ErrCode.
var (
	ErrCode_name = map[int32]string{
		0: "WRONG_LEADER",
		1: "OK",
		2: "NO_KEY",
		3: "INVALID_SESSION",
		4: "INVALID_OP",
		5: "INVALID_TOKEN",
	}
	ErrCode_value = map[string]int32{
		"WRONG_LEADER":    0,
		"OK":              1,
		"NO_KEY":          2,
		"INVALID_SESSION": 3,
		"INVALID_OP":      4,
		"INVALID_TOKEN":   5,
	}
)

func (x ErrCode) Enum() *ErrCode {
	p := new(ErrCode)
	*p = x
	return p
}

func (x ErrCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrCode) Descriptor() protoreflect.EnumDescriptor {
	return file_kvserver_proto_enumTypes[0].Descriptor()
}

func (ErrCode) Type() protoreflect.EnumType {
	return &file_kvserver_proto_enumTypes[0]
}

func (x ErrCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrCode.Descriptor instead.
func (ErrCode) EnumDescriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{0}
}

type Op int32

const (
	Op_PUT    Op = 0
	Op_APPEND Op = 1
	Op_DELETE Op = 2
)

// Enum value maps for Op.
var (
	Op_name = map[int32]string{
		0: "PUT",
		1: "APPEND",
		2: "DELETE",
	}
	Op_value = map[string]int32{
		"PUT":    0,
		"APPEND": 1,
		"DELETE": 2,
	}
)

func (x Op) Enum() *Op {
	p := new(Op)
	*p = x
	return p
}

func (x Op) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Op) Descriptor() protoreflect.EnumDescriptor {
	return file_kvserver_proto_enumTypes[1].Descriptor()
}

func (Op) Type() protoreflect.EnumType {
	return &file_kvserver_proto_enumTypes[1]
}

func (x Op) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Op.Descriptor instead.
func (Op) EnumDescriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{1}
}

type ClearSessionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Token string `protobuf:"bytes,1,opt,name=token,proto3" json:"token,omitempty"`
}

func (x *ClearSessionRequest) Reset() {
	*x = ClearSessionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ClearSessionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClearSessionRequest) ProtoMessage() {}

func (x *ClearSessionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClearSessionRequest.ProtoReflect.Descriptor instead.
func (*ClearSessionRequest) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{0}
}

func (x *ClearSessionRequest) GetToken() string {
	if x != nil {
		return x.Token
	}
	return ""
}

type ClearSessionReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ErrCode ErrCode `protobuf:"varint,1,opt,name=err_code,json=errCode,proto3,enum=ErrCode" json:"err_code,omitempty"`
}

func (x *ClearSessionReply) Reset() {
	*x = ClearSessionReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ClearSessionReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClearSessionReply) ProtoMessage() {}

func (x *ClearSessionReply) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClearSessionReply.ProtoReflect.Descriptor instead.
func (*ClearSessionReply) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{1}
}

func (x *ClearSessionReply) GetErrCode() ErrCode {
	if x != nil {
		return x.ErrCode
	}
	return ErrCode_WRONG_LEADER
}

type OpenSessionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *OpenSessionRequest) Reset() {
	*x = OpenSessionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenSessionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenSessionRequest) ProtoMessage() {}

func (x *OpenSessionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenSessionRequest.ProtoReflect.Descriptor instead.
func (*OpenSessionRequest) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{2}
}

type OpenSessionReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ClientId int64   `protobuf:"varint,1,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`
	ErrCode  ErrCode `protobuf:"varint,2,opt,name=err_code,json=errCode,proto3,enum=ErrCode" json:"err_code,omitempty"`
}

func (x *OpenSessionReply) Reset() {
	*x = OpenSessionReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenSessionReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenSessionReply) ProtoMessage() {}

func (x *OpenSessionReply) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenSessionReply.ProtoReflect.Descriptor instead.
func (*OpenSessionReply) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{3}
}

func (x *OpenSessionReply) GetClientId() int64 {
	if x != nil {
		return x.ClientId
	}
	return 0
}

func (x *OpenSessionReply) GetErrCode() ErrCode {
	if x != nil {
		return x.ErrCode
	}
	return ErrCode_WRONG_LEADER
}

type UpdateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value     string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Op        Op     `protobuf:"varint,3,opt,name=op,proto3,enum=Op" json:"op,omitempty"`                        // "Put" or "Append" or "Delete"
	ClientId  int64  `protobuf:"varint,4,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`    // client id
	RequestId int64  `protobuf:"varint,5,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"` // client request id (increase monotonically)
}

func (x *UpdateRequest) Reset() {
	*x = UpdateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UpdateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateRequest) ProtoMessage() {}

func (x *UpdateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateRequest.ProtoReflect.Descriptor instead.
func (*UpdateRequest) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{4}
}

func (x *UpdateRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *UpdateRequest) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *UpdateRequest) GetOp() Op {
	if x != nil {
		return x.Op
	}
	return Op_PUT
}

func (x *UpdateRequest) GetClientId() int64 {
	if x != nil {
		return x.ClientId
	}
	return 0
}

func (x *UpdateRequest) GetRequestId() int64 {
	if x != nil {
		return x.RequestId
	}
	return 0
}

type UpdateReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ErrCode ErrCode `protobuf:"varint,1,opt,name=err_code,json=errCode,proto3,enum=ErrCode" json:"err_code,omitempty"`
}

func (x *UpdateReply) Reset() {
	*x = UpdateReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UpdateReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateReply) ProtoMessage() {}

func (x *UpdateReply) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateReply.ProtoReflect.Descriptor instead.
func (*UpdateReply) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{5}
}

func (x *UpdateReply) GetErrCode() ErrCode {
	if x != nil {
		return x.ErrCode
	}
	return ErrCode_WRONG_LEADER
}

type GetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ClientId  int64  `protobuf:"varint,2,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`    // client id
	RequestId int64  `protobuf:"varint,3,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"` // client request id (increase monotonically)
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRequest.ProtoReflect.Descriptor instead.
func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{6}
}

func (x *GetRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *GetRequest) GetClientId() int64 {
	if x != nil {
		return x.ClientId
	}
	return 0
}

func (x *GetRequest) GetRequestId() int64 {
	if x != nil {
		return x.RequestId
	}
	return 0
}

type GetReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value   string  `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	ErrCode ErrCode `protobuf:"varint,1,opt,name=err_code,json=errCode,proto3,enum=ErrCode" json:"err_code,omitempty"`
}

func (x *GetReply) Reset() {
	*x = GetReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kvserver_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetReply) ProtoMessage() {}

func (x *GetReply) ProtoReflect() protoreflect.Message {
	mi := &file_kvserver_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetReply.ProtoReflect.Descriptor instead.
func (*GetReply) Descriptor() ([]byte, []int) {
	return file_kvserver_proto_rawDescGZIP(), []int{7}
}

func (x *GetReply) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *GetReply) GetErrCode() ErrCode {
	if x != nil {
		return x.ErrCode
	}
	return ErrCode_WRONG_LEADER
}

var File_kvserver_proto protoreflect.FileDescriptor

var file_kvserver_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x6b, 0x76, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x2b, 0x0a, 0x13, 0x43, 0x6c, 0x65, 0x61, 0x72, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x22, 0x38, 0x0a,
	0x11, 0x43, 0x6c, 0x65, 0x61, 0x72, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x70,
	0x6c, 0x79, 0x12, 0x23, 0x0a, 0x08, 0x65, 0x72, 0x72, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x08, 0x2e, 0x45, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x07,
	0x65, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x22, 0x14, 0x0a, 0x12, 0x4f, 0x70, 0x65, 0x6e, 0x53,
	0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x54, 0x0a,
	0x10, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x70, 0x6c,
	0x79, 0x12, 0x1b, 0x0a, 0x09, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x23,
	0x0a, 0x08, 0x65, 0x72, 0x72, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x08, 0x2e, 0x45, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x07, 0x65, 0x72, 0x72, 0x43,
	0x6f, 0x64, 0x65, 0x22, 0x88, 0x01, 0x0a, 0x0d, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x13, 0x0a,
	0x02, 0x6f, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x03, 0x2e, 0x4f, 0x70, 0x52, 0x02,
	0x6f, 0x70, 0x12, 0x1b, 0x0a, 0x09, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12,
	0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22, 0x32,
	0x0a, 0x0b, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x23, 0x0a,
	0x08, 0x65, 0x72, 0x72, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x08, 0x2e, 0x45, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x07, 0x65, 0x72, 0x72, 0x43, 0x6f,
	0x64, 0x65, 0x22, 0x5a, 0x0a, 0x0a, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x1b, 0x0a, 0x09, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12,
	0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22, 0x45,
	0x0a, 0x08, 0x47, 0x65, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x12, 0x23, 0x0a, 0x08, 0x65, 0x72, 0x72, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x08, 0x2e, 0x45, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x07, 0x65, 0x72,
	0x72, 0x43, 0x6f, 0x64, 0x65, 0x2a, 0x67, 0x0a, 0x07, 0x45, 0x72, 0x72, 0x43, 0x6f, 0x64, 0x65,
	0x12, 0x10, 0x0a, 0x0c, 0x57, 0x52, 0x4f, 0x4e, 0x47, 0x5f, 0x4c, 0x45, 0x41, 0x44, 0x45, 0x52,
	0x10, 0x00, 0x12, 0x06, 0x0a, 0x02, 0x4f, 0x4b, 0x10, 0x01, 0x12, 0x0a, 0x0a, 0x06, 0x4e, 0x4f,
	0x5f, 0x4b, 0x45, 0x59, 0x10, 0x02, 0x12, 0x13, 0x0a, 0x0f, 0x49, 0x4e, 0x56, 0x41, 0x4c, 0x49,
	0x44, 0x5f, 0x53, 0x45, 0x53, 0x53, 0x49, 0x4f, 0x4e, 0x10, 0x03, 0x12, 0x0e, 0x0a, 0x0a, 0x49,
	0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x4f, 0x50, 0x10, 0x04, 0x12, 0x11, 0x0a, 0x0d, 0x49,
	0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x54, 0x4f, 0x4b, 0x45, 0x4e, 0x10, 0x05, 0x2a, 0x25,
	0x0a, 0x02, 0x4f, 0x70, 0x12, 0x07, 0x0a, 0x03, 0x50, 0x55, 0x54, 0x10, 0x00, 0x12, 0x0a, 0x0a,
	0x06, 0x41, 0x50, 0x50, 0x45, 0x4e, 0x44, 0x10, 0x01, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x45, 0x4c,
	0x45, 0x54, 0x45, 0x10, 0x02, 0x32, 0xc2, 0x01, 0x0a, 0x08, 0x4b, 0x56, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x12, 0x38, 0x0a, 0x0c, 0x43, 0x6c, 0x65, 0x61, 0x72, 0x53, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x14, 0x2e, 0x43, 0x6c, 0x65, 0x61, 0x72, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x12, 0x2e, 0x43, 0x6c, 0x65, 0x61, 0x72,
	0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x35, 0x0a, 0x0b,
	0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x13, 0x2e, 0x4f, 0x70,
	0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x11, 0x2e, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x70, 0x6c, 0x79, 0x12, 0x1d, 0x0a, 0x03, 0x47, 0x65, 0x74, 0x12, 0x0b, 0x2e, 0x47, 0x65, 0x74,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x09, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x70,
	0x6c, 0x79, 0x12, 0x26, 0x0a, 0x06, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x12, 0x0e, 0x2e, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0c, 0x2e, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x42, 0x0c, 0x5a, 0x0a, 0x2e, 0x3b,
	0x6b, 0x76, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kvserver_proto_rawDescOnce sync.Once
	file_kvserver_proto_rawDescData = file_kvserver_proto_rawDesc
)

func file_kvserver_proto_rawDescGZIP() []byte {
	file_kvserver_proto_rawDescOnce.Do(func() {
		file_kvserver_proto_rawDescData = protoimpl.X.CompressGZIP(file_kvserver_proto_rawDescData)
	})
	return file_kvserver_proto_rawDescData
}

var file_kvserver_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_kvserver_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_kvserver_proto_goTypes = []interface{}{
	(ErrCode)(0),                // 0: ErrCode
	(Op)(0),                     // 1: Op
	(*ClearSessionRequest)(nil), // 2: ClearSessionRequest
	(*ClearSessionReply)(nil),   // 3: ClearSessionReply
	(*OpenSessionRequest)(nil),  // 4: OpenSessionRequest
	(*OpenSessionReply)(nil),    // 5: OpenSessionReply
	(*UpdateRequest)(nil),       // 6: UpdateRequest
	(*UpdateReply)(nil),         // 7: UpdateReply
	(*GetRequest)(nil),          // 8: GetRequest
	(*GetReply)(nil),            // 9: GetReply
}
var file_kvserver_proto_depIdxs = []int32{
	0, // 0: ClearSessionReply.err_code:type_name -> ErrCode
	0, // 1: OpenSessionReply.err_code:type_name -> ErrCode
	1, // 2: UpdateRequest.op:type_name -> Op
	0, // 3: UpdateReply.err_code:type_name -> ErrCode
	0, // 4: GetReply.err_code:type_name -> ErrCode
	2, // 5: KVServer.ClearSession:input_type -> ClearSessionRequest
	4, // 6: KVServer.OpenSession:input_type -> OpenSessionRequest
	8, // 7: KVServer.Get:input_type -> GetRequest
	6, // 8: KVServer.Update:input_type -> UpdateRequest
	3, // 9: KVServer.ClearSession:output_type -> ClearSessionReply
	5, // 10: KVServer.OpenSession:output_type -> OpenSessionReply
	9, // 11: KVServer.Get:output_type -> GetReply
	7, // 12: KVServer.Update:output_type -> UpdateReply
	9, // [9:13] is the sub-list for method output_type
	5, // [5:9] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_kvserver_proto_init() }
func file_kvserver_proto_init() {
	if File_kvserver_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_kvserver_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ClearSessionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ClearSessionReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenSessionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenSessionReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UpdateRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UpdateReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kvserver_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kvserver_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_kvserver_proto_goTypes,
		DependencyIndexes: file_kvserver_proto_depIdxs,
		EnumInfos:         file_kvserver_proto_enumTypes,
		MessageInfos:      file_kvserver_proto_msgTypes,
	}.Build()
	File_kvserver_proto = out.File
	file_kvserver_proto_rawDesc = nil
	file_kvserver_proto_goTypes = nil
	file_kvserver_proto_depIdxs = nil
}
