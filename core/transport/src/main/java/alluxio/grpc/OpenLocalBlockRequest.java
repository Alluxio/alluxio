// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_worker.proto

package alluxio.grpc;

/**
 * <pre>
 * next available id: 3
 * </pre>
 *
 * Protobuf type {@code alluxio.grpc.block.OpenLocalBlockRequest}
 */
public  final class OpenLocalBlockRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.block.OpenLocalBlockRequest)
    OpenLocalBlockRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use OpenLocalBlockRequest.newBuilder() to construct.
  private OpenLocalBlockRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private OpenLocalBlockRequest() {
    blockId_ = 0L;
    promote_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private OpenLocalBlockRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 8: {
            bitField0_ |= 0x00000001;
            blockId_ = input.readInt64();
            break;
          }
          case 16: {
            bitField0_ |= 0x00000002;
            promote_ = input.readBool();
            break;
          }
          case 8010: {
            alluxio.proto.security.CapabilityProto.Capability.Builder subBuilder = null;
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
              subBuilder = capability_.toBuilder();
            }
            capability_ = input.readMessage(alluxio.proto.security.CapabilityProto.Capability.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(capability_);
              capability_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000004;
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_OpenLocalBlockRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_OpenLocalBlockRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.OpenLocalBlockRequest.class, alluxio.grpc.OpenLocalBlockRequest.Builder.class);
  }

  private int bitField0_;
  public static final int BLOCK_ID_FIELD_NUMBER = 1;
  private long blockId_;
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  public boolean hasBlockId() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  public long getBlockId() {
    return blockId_;
  }

  public static final int PROMOTE_FIELD_NUMBER = 2;
  private boolean promote_;
  /**
   * <code>optional bool promote = 2;</code>
   */
  public boolean hasPromote() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional bool promote = 2;</code>
   */
  public boolean getPromote() {
    return promote_;
  }

  public static final int CAPABILITY_FIELD_NUMBER = 1001;
  private alluxio.proto.security.CapabilityProto.Capability capability_;
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
   */
  public boolean hasCapability() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
   */
  public alluxio.proto.security.CapabilityProto.Capability getCapability() {
    return capability_ == null ? alluxio.proto.security.CapabilityProto.Capability.getDefaultInstance() : capability_;
  }
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
   */
  public alluxio.proto.security.CapabilityProto.CapabilityOrBuilder getCapabilityOrBuilder() {
    return capability_ == null ? alluxio.proto.security.CapabilityProto.Capability.getDefaultInstance() : capability_;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeInt64(1, blockId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeBool(2, promote_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeMessage(1001, getCapability());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, blockId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(2, promote_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1001, getCapability());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof alluxio.grpc.OpenLocalBlockRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.OpenLocalBlockRequest other = (alluxio.grpc.OpenLocalBlockRequest) obj;

    boolean result = true;
    result = result && (hasBlockId() == other.hasBlockId());
    if (hasBlockId()) {
      result = result && (getBlockId()
          == other.getBlockId());
    }
    result = result && (hasPromote() == other.hasPromote());
    if (hasPromote()) {
      result = result && (getPromote()
          == other.getPromote());
    }
    result = result && (hasCapability() == other.hasCapability());
    if (hasCapability()) {
      result = result && getCapability()
          .equals(other.getCapability());
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasBlockId()) {
      hash = (37 * hash) + BLOCK_ID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getBlockId());
    }
    if (hasPromote()) {
      hash = (37 * hash) + PROMOTE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getPromote());
    }
    if (hasCapability()) {
      hash = (37 * hash) + CAPABILITY_FIELD_NUMBER;
      hash = (53 * hash) + getCapability().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenLocalBlockRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(alluxio.grpc.OpenLocalBlockRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * <pre>
   * next available id: 3
   * </pre>
   *
   * Protobuf type {@code alluxio.grpc.block.OpenLocalBlockRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.block.OpenLocalBlockRequest)
      alluxio.grpc.OpenLocalBlockRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_OpenLocalBlockRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_OpenLocalBlockRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.OpenLocalBlockRequest.class, alluxio.grpc.OpenLocalBlockRequest.Builder.class);
    }

    // Construct using alluxio.grpc.OpenLocalBlockRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getCapabilityFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      blockId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      promote_ = false;
      bitField0_ = (bitField0_ & ~0x00000002);
      if (capabilityBuilder_ == null) {
        capability_ = null;
      } else {
        capabilityBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_OpenLocalBlockRequest_descriptor;
    }

    public alluxio.grpc.OpenLocalBlockRequest getDefaultInstanceForType() {
      return alluxio.grpc.OpenLocalBlockRequest.getDefaultInstance();
    }

    public alluxio.grpc.OpenLocalBlockRequest build() {
      alluxio.grpc.OpenLocalBlockRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.OpenLocalBlockRequest buildPartial() {
      alluxio.grpc.OpenLocalBlockRequest result = new alluxio.grpc.OpenLocalBlockRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.blockId_ = blockId_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.promote_ = promote_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      if (capabilityBuilder_ == null) {
        result.capability_ = capability_;
      } else {
        result.capability_ = capabilityBuilder_.build();
      }
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof alluxio.grpc.OpenLocalBlockRequest) {
        return mergeFrom((alluxio.grpc.OpenLocalBlockRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.OpenLocalBlockRequest other) {
      if (other == alluxio.grpc.OpenLocalBlockRequest.getDefaultInstance()) return this;
      if (other.hasBlockId()) {
        setBlockId(other.getBlockId());
      }
      if (other.hasPromote()) {
        setPromote(other.getPromote());
      }
      if (other.hasCapability()) {
        mergeCapability(other.getCapability());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.OpenLocalBlockRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.OpenLocalBlockRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private long blockId_ ;
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public boolean hasBlockId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public long getBlockId() {
      return blockId_;
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public Builder setBlockId(long value) {
      bitField0_ |= 0x00000001;
      blockId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public Builder clearBlockId() {
      bitField0_ = (bitField0_ & ~0x00000001);
      blockId_ = 0L;
      onChanged();
      return this;
    }

    private boolean promote_ ;
    /**
     * <code>optional bool promote = 2;</code>
     */
    public boolean hasPromote() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bool promote = 2;</code>
     */
    public boolean getPromote() {
      return promote_;
    }
    /**
     * <code>optional bool promote = 2;</code>
     */
    public Builder setPromote(boolean value) {
      bitField0_ |= 0x00000002;
      promote_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool promote = 2;</code>
     */
    public Builder clearPromote() {
      bitField0_ = (bitField0_ & ~0x00000002);
      promote_ = false;
      onChanged();
      return this;
    }

    private alluxio.proto.security.CapabilityProto.Capability capability_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.proto.security.CapabilityProto.Capability, alluxio.proto.security.CapabilityProto.Capability.Builder, alluxio.proto.security.CapabilityProto.CapabilityOrBuilder> capabilityBuilder_;
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public boolean hasCapability() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public alluxio.proto.security.CapabilityProto.Capability getCapability() {
      if (capabilityBuilder_ == null) {
        return capability_ == null ? alluxio.proto.security.CapabilityProto.Capability.getDefaultInstance() : capability_;
      } else {
        return capabilityBuilder_.getMessage();
      }
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public Builder setCapability(alluxio.proto.security.CapabilityProto.Capability value) {
      if (capabilityBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        capability_ = value;
        onChanged();
      } else {
        capabilityBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public Builder setCapability(
        alluxio.proto.security.CapabilityProto.Capability.Builder builderForValue) {
      if (capabilityBuilder_ == null) {
        capability_ = builderForValue.build();
        onChanged();
      } else {
        capabilityBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public Builder mergeCapability(alluxio.proto.security.CapabilityProto.Capability value) {
      if (capabilityBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004) &&
            capability_ != null &&
            capability_ != alluxio.proto.security.CapabilityProto.Capability.getDefaultInstance()) {
          capability_ =
            alluxio.proto.security.CapabilityProto.Capability.newBuilder(capability_).mergeFrom(value).buildPartial();
        } else {
          capability_ = value;
        }
        onChanged();
      } else {
        capabilityBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public Builder clearCapability() {
      if (capabilityBuilder_ == null) {
        capability_ = null;
        onChanged();
      } else {
        capabilityBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public alluxio.proto.security.CapabilityProto.Capability.Builder getCapabilityBuilder() {
      bitField0_ |= 0x00000004;
      onChanged();
      return getCapabilityFieldBuilder().getBuilder();
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    public alluxio.proto.security.CapabilityProto.CapabilityOrBuilder getCapabilityOrBuilder() {
      if (capabilityBuilder_ != null) {
        return capabilityBuilder_.getMessageOrBuilder();
      } else {
        return capability_ == null ?
            alluxio.proto.security.CapabilityProto.Capability.getDefaultInstance() : capability_;
      }
    }
    /**
     * <pre>
     * ALLUXIO CS ADD
     * </pre>
     *
     * <code>optional .alluxio.proto.security.Capability capability = 1001;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.proto.security.CapabilityProto.Capability, alluxio.proto.security.CapabilityProto.Capability.Builder, alluxio.proto.security.CapabilityProto.CapabilityOrBuilder> 
        getCapabilityFieldBuilder() {
      if (capabilityBuilder_ == null) {
        capabilityBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.proto.security.CapabilityProto.Capability, alluxio.proto.security.CapabilityProto.Capability.Builder, alluxio.proto.security.CapabilityProto.CapabilityOrBuilder>(
                getCapability(),
                getParentForChildren(),
                isClean());
        capability_ = null;
      }
      return capabilityBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.block.OpenLocalBlockRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.block.OpenLocalBlockRequest)
  private static final alluxio.grpc.OpenLocalBlockRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.OpenLocalBlockRequest();
  }

  public static alluxio.grpc.OpenLocalBlockRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<OpenLocalBlockRequest>
      PARSER = new com.google.protobuf.AbstractParser<OpenLocalBlockRequest>() {
    public OpenLocalBlockRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new OpenLocalBlockRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<OpenLocalBlockRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<OpenLocalBlockRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.OpenLocalBlockRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

