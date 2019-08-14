// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_worker.proto

package alluxio.grpc;

/**
 * <pre>
 * next available id: 9
 * </pre>
 *
 * Protobuf type {@code alluxio.grpc.block.CreateLocalBlockRequest}
 */
public  final class CreateLocalBlockRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.block.CreateLocalBlockRequest)
    CreateLocalBlockRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use CreateLocalBlockRequest.newBuilder() to construct.
  private CreateLocalBlockRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private CreateLocalBlockRequest() {
    blockId_ = 0L;
    tier_ = 0;
    spaceToReserve_ = 0L;
    onlyReserveSpace_ = false;
    cleanupOnFailure_ = false;
    mediumType_ = "";
    pinOnCreate_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private CreateLocalBlockRequest(
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
          case 24: {
            bitField0_ |= 0x00000002;
            tier_ = input.readInt32();
            break;
          }
          case 32: {
            bitField0_ |= 0x00000004;
            spaceToReserve_ = input.readInt64();
            break;
          }
          case 40: {
            bitField0_ |= 0x00000008;
            onlyReserveSpace_ = input.readBool();
            break;
          }
          case 48: {
            bitField0_ |= 0x00000010;
            cleanupOnFailure_ = input.readBool();
            break;
          }
          case 58: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000020;
            mediumType_ = bs;
            break;
          }
          case 64: {
            bitField0_ |= 0x00000040;
            pinOnCreate_ = input.readBool();
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
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_CreateLocalBlockRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_CreateLocalBlockRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.CreateLocalBlockRequest.class, alluxio.grpc.CreateLocalBlockRequest.Builder.class);
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

  public static final int TIER_FIELD_NUMBER = 3;
  private int tier_;
  /**
   * <code>optional int32 tier = 3;</code>
   */
  public boolean hasTier() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional int32 tier = 3;</code>
   */
  public int getTier() {
    return tier_;
  }

  public static final int SPACE_TO_RESERVE_FIELD_NUMBER = 4;
  private long spaceToReserve_;
  /**
   * <code>optional int64 space_to_reserve = 4;</code>
   */
  public boolean hasSpaceToReserve() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional int64 space_to_reserve = 4;</code>
   */
  public long getSpaceToReserve() {
    return spaceToReserve_;
  }

  public static final int ONLY_RESERVE_SPACE_FIELD_NUMBER = 5;
  private boolean onlyReserveSpace_;
  /**
   * <pre>
   * If set, only reserve space for the block.
   * </pre>
   *
   * <code>optional bool only_reserve_space = 5;</code>
   */
  public boolean hasOnlyReserveSpace() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <pre>
   * If set, only reserve space for the block.
   * </pre>
   *
   * <code>optional bool only_reserve_space = 5;</code>
   */
  public boolean getOnlyReserveSpace() {
    return onlyReserveSpace_;
  }

  public static final int CLEANUP_ON_FAILURE_FIELD_NUMBER = 6;
  private boolean cleanupOnFailure_;
  /**
   * <code>optional bool cleanup_on_failure = 6;</code>
   */
  public boolean hasCleanupOnFailure() {
    return ((bitField0_ & 0x00000010) == 0x00000010);
  }
  /**
   * <code>optional bool cleanup_on_failure = 6;</code>
   */
  public boolean getCleanupOnFailure() {
    return cleanupOnFailure_;
  }

  public static final int MEDIUM_TYPE_FIELD_NUMBER = 7;
  private volatile java.lang.Object mediumType_;
  /**
   * <code>optional string medium_type = 7;</code>
   */
  public boolean hasMediumType() {
    return ((bitField0_ & 0x00000020) == 0x00000020);
  }
  /**
   * <code>optional string medium_type = 7;</code>
   */
  public java.lang.String getMediumType() {
    java.lang.Object ref = mediumType_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        mediumType_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string medium_type = 7;</code>
   */
  public com.google.protobuf.ByteString
      getMediumTypeBytes() {
    java.lang.Object ref = mediumType_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      mediumType_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PIN_ON_CREATE_FIELD_NUMBER = 8;
  private boolean pinOnCreate_;
  /**
   * <code>optional bool pin_on_create = 8;</code>
   */
  public boolean hasPinOnCreate() {
    return ((bitField0_ & 0x00000040) == 0x00000040);
  }
  /**
   * <code>optional bool pin_on_create = 8;</code>
   */
  public boolean getPinOnCreate() {
    return pinOnCreate_;
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
      output.writeInt32(3, tier_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeInt64(4, spaceToReserve_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeBool(5, onlyReserveSpace_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      output.writeBool(6, cleanupOnFailure_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 7, mediumType_);
    }
    if (((bitField0_ & 0x00000040) == 0x00000040)) {
      output.writeBool(8, pinOnCreate_);
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
        .computeInt32Size(3, tier_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(4, spaceToReserve_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(5, onlyReserveSpace_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(6, cleanupOnFailure_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(7, mediumType_);
    }
    if (((bitField0_ & 0x00000040) == 0x00000040)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(8, pinOnCreate_);
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
    if (!(obj instanceof alluxio.grpc.CreateLocalBlockRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.CreateLocalBlockRequest other = (alluxio.grpc.CreateLocalBlockRequest) obj;

    boolean result = true;
    result = result && (hasBlockId() == other.hasBlockId());
    if (hasBlockId()) {
      result = result && (getBlockId()
          == other.getBlockId());
    }
    result = result && (hasTier() == other.hasTier());
    if (hasTier()) {
      result = result && (getTier()
          == other.getTier());
    }
    result = result && (hasSpaceToReserve() == other.hasSpaceToReserve());
    if (hasSpaceToReserve()) {
      result = result && (getSpaceToReserve()
          == other.getSpaceToReserve());
    }
    result = result && (hasOnlyReserveSpace() == other.hasOnlyReserveSpace());
    if (hasOnlyReserveSpace()) {
      result = result && (getOnlyReserveSpace()
          == other.getOnlyReserveSpace());
    }
    result = result && (hasCleanupOnFailure() == other.hasCleanupOnFailure());
    if (hasCleanupOnFailure()) {
      result = result && (getCleanupOnFailure()
          == other.getCleanupOnFailure());
    }
    result = result && (hasMediumType() == other.hasMediumType());
    if (hasMediumType()) {
      result = result && getMediumType()
          .equals(other.getMediumType());
    }
    result = result && (hasPinOnCreate() == other.hasPinOnCreate());
    if (hasPinOnCreate()) {
      result = result && (getPinOnCreate()
          == other.getPinOnCreate());
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
    if (hasTier()) {
      hash = (37 * hash) + TIER_FIELD_NUMBER;
      hash = (53 * hash) + getTier();
    }
    if (hasSpaceToReserve()) {
      hash = (37 * hash) + SPACE_TO_RESERVE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getSpaceToReserve());
    }
    if (hasOnlyReserveSpace()) {
      hash = (37 * hash) + ONLY_RESERVE_SPACE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getOnlyReserveSpace());
    }
    if (hasCleanupOnFailure()) {
      hash = (37 * hash) + CLEANUP_ON_FAILURE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getCleanupOnFailure());
    }
    if (hasMediumType()) {
      hash = (37 * hash) + MEDIUM_TYPE_FIELD_NUMBER;
      hash = (53 * hash) + getMediumType().hashCode();
    }
    if (hasPinOnCreate()) {
      hash = (37 * hash) + PIN_ON_CREATE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getPinOnCreate());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CreateLocalBlockRequest parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.CreateLocalBlockRequest prototype) {
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
   * next available id: 9
   * </pre>
   *
   * Protobuf type {@code alluxio.grpc.block.CreateLocalBlockRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.block.CreateLocalBlockRequest)
      alluxio.grpc.CreateLocalBlockRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_CreateLocalBlockRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_CreateLocalBlockRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.CreateLocalBlockRequest.class, alluxio.grpc.CreateLocalBlockRequest.Builder.class);
    }

    // Construct using alluxio.grpc.CreateLocalBlockRequest.newBuilder()
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
      }
    }
    public Builder clear() {
      super.clear();
      blockId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      tier_ = 0;
      bitField0_ = (bitField0_ & ~0x00000002);
      spaceToReserve_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000004);
      onlyReserveSpace_ = false;
      bitField0_ = (bitField0_ & ~0x00000008);
      cleanupOnFailure_ = false;
      bitField0_ = (bitField0_ & ~0x00000010);
      mediumType_ = "";
      bitField0_ = (bitField0_ & ~0x00000020);
      pinOnCreate_ = false;
      bitField0_ = (bitField0_ & ~0x00000040);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.BlockWorkerProto.internal_static_alluxio_grpc_block_CreateLocalBlockRequest_descriptor;
    }

    public alluxio.grpc.CreateLocalBlockRequest getDefaultInstanceForType() {
      return alluxio.grpc.CreateLocalBlockRequest.getDefaultInstance();
    }

    public alluxio.grpc.CreateLocalBlockRequest build() {
      alluxio.grpc.CreateLocalBlockRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.CreateLocalBlockRequest buildPartial() {
      alluxio.grpc.CreateLocalBlockRequest result = new alluxio.grpc.CreateLocalBlockRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.blockId_ = blockId_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.tier_ = tier_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.spaceToReserve_ = spaceToReserve_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.onlyReserveSpace_ = onlyReserveSpace_;
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000010;
      }
      result.cleanupOnFailure_ = cleanupOnFailure_;
      if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
        to_bitField0_ |= 0x00000020;
      }
      result.mediumType_ = mediumType_;
      if (((from_bitField0_ & 0x00000040) == 0x00000040)) {
        to_bitField0_ |= 0x00000040;
      }
      result.pinOnCreate_ = pinOnCreate_;
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
      if (other instanceof alluxio.grpc.CreateLocalBlockRequest) {
        return mergeFrom((alluxio.grpc.CreateLocalBlockRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.CreateLocalBlockRequest other) {
      if (other == alluxio.grpc.CreateLocalBlockRequest.getDefaultInstance()) return this;
      if (other.hasBlockId()) {
        setBlockId(other.getBlockId());
      }
      if (other.hasTier()) {
        setTier(other.getTier());
      }
      if (other.hasSpaceToReserve()) {
        setSpaceToReserve(other.getSpaceToReserve());
      }
      if (other.hasOnlyReserveSpace()) {
        setOnlyReserveSpace(other.getOnlyReserveSpace());
      }
      if (other.hasCleanupOnFailure()) {
        setCleanupOnFailure(other.getCleanupOnFailure());
      }
      if (other.hasMediumType()) {
        bitField0_ |= 0x00000020;
        mediumType_ = other.mediumType_;
        onChanged();
      }
      if (other.hasPinOnCreate()) {
        setPinOnCreate(other.getPinOnCreate());
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
      alluxio.grpc.CreateLocalBlockRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.CreateLocalBlockRequest) e.getUnfinishedMessage();
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

    private int tier_ ;
    /**
     * <code>optional int32 tier = 3;</code>
     */
    public boolean hasTier() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int32 tier = 3;</code>
     */
    public int getTier() {
      return tier_;
    }
    /**
     * <code>optional int32 tier = 3;</code>
     */
    public Builder setTier(int value) {
      bitField0_ |= 0x00000002;
      tier_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 tier = 3;</code>
     */
    public Builder clearTier() {
      bitField0_ = (bitField0_ & ~0x00000002);
      tier_ = 0;
      onChanged();
      return this;
    }

    private long spaceToReserve_ ;
    /**
     * <code>optional int64 space_to_reserve = 4;</code>
     */
    public boolean hasSpaceToReserve() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int64 space_to_reserve = 4;</code>
     */
    public long getSpaceToReserve() {
      return spaceToReserve_;
    }
    /**
     * <code>optional int64 space_to_reserve = 4;</code>
     */
    public Builder setSpaceToReserve(long value) {
      bitField0_ |= 0x00000004;
      spaceToReserve_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 space_to_reserve = 4;</code>
     */
    public Builder clearSpaceToReserve() {
      bitField0_ = (bitField0_ & ~0x00000004);
      spaceToReserve_ = 0L;
      onChanged();
      return this;
    }

    private boolean onlyReserveSpace_ ;
    /**
     * <pre>
     * If set, only reserve space for the block.
     * </pre>
     *
     * <code>optional bool only_reserve_space = 5;</code>
     */
    public boolean hasOnlyReserveSpace() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <pre>
     * If set, only reserve space for the block.
     * </pre>
     *
     * <code>optional bool only_reserve_space = 5;</code>
     */
    public boolean getOnlyReserveSpace() {
      return onlyReserveSpace_;
    }
    /**
     * <pre>
     * If set, only reserve space for the block.
     * </pre>
     *
     * <code>optional bool only_reserve_space = 5;</code>
     */
    public Builder setOnlyReserveSpace(boolean value) {
      bitField0_ |= 0x00000008;
      onlyReserveSpace_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * If set, only reserve space for the block.
     * </pre>
     *
     * <code>optional bool only_reserve_space = 5;</code>
     */
    public Builder clearOnlyReserveSpace() {
      bitField0_ = (bitField0_ & ~0x00000008);
      onlyReserveSpace_ = false;
      onChanged();
      return this;
    }

    private boolean cleanupOnFailure_ ;
    /**
     * <code>optional bool cleanup_on_failure = 6;</code>
     */
    public boolean hasCleanupOnFailure() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional bool cleanup_on_failure = 6;</code>
     */
    public boolean getCleanupOnFailure() {
      return cleanupOnFailure_;
    }
    /**
     * <code>optional bool cleanup_on_failure = 6;</code>
     */
    public Builder setCleanupOnFailure(boolean value) {
      bitField0_ |= 0x00000010;
      cleanupOnFailure_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool cleanup_on_failure = 6;</code>
     */
    public Builder clearCleanupOnFailure() {
      bitField0_ = (bitField0_ & ~0x00000010);
      cleanupOnFailure_ = false;
      onChanged();
      return this;
    }

    private java.lang.Object mediumType_ = "";
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public boolean hasMediumType() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public java.lang.String getMediumType() {
      java.lang.Object ref = mediumType_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          mediumType_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public com.google.protobuf.ByteString
        getMediumTypeBytes() {
      java.lang.Object ref = mediumType_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        mediumType_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public Builder setMediumType(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
      mediumType_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public Builder clearMediumType() {
      bitField0_ = (bitField0_ & ~0x00000020);
      mediumType_ = getDefaultInstance().getMediumType();
      onChanged();
      return this;
    }
    /**
     * <code>optional string medium_type = 7;</code>
     */
    public Builder setMediumTypeBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
      mediumType_ = value;
      onChanged();
      return this;
    }

    private boolean pinOnCreate_ ;
    /**
     * <code>optional bool pin_on_create = 8;</code>
     */
    public boolean hasPinOnCreate() {
      return ((bitField0_ & 0x00000040) == 0x00000040);
    }
    /**
     * <code>optional bool pin_on_create = 8;</code>
     */
    public boolean getPinOnCreate() {
      return pinOnCreate_;
    }
    /**
     * <code>optional bool pin_on_create = 8;</code>
     */
    public Builder setPinOnCreate(boolean value) {
      bitField0_ |= 0x00000040;
      pinOnCreate_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool pin_on_create = 8;</code>
     */
    public Builder clearPinOnCreate() {
      bitField0_ = (bitField0_ & ~0x00000040);
      pinOnCreate_ = false;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.block.CreateLocalBlockRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.block.CreateLocalBlockRequest)
  private static final alluxio.grpc.CreateLocalBlockRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.CreateLocalBlockRequest();
  }

  public static alluxio.grpc.CreateLocalBlockRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<CreateLocalBlockRequest>
      PARSER = new com.google.protobuf.AbstractParser<CreateLocalBlockRequest>() {
    public CreateLocalBlockRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new CreateLocalBlockRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<CreateLocalBlockRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<CreateLocalBlockRequest> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.CreateLocalBlockRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

