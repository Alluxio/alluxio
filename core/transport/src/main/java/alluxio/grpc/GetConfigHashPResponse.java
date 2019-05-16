// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.meta.GetConfigHashPResponse}
 */
public  final class GetConfigHashPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.meta.GetConfigHashPResponse)
    GetConfigHashPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetConfigHashPResponse.newBuilder() to construct.
  private GetConfigHashPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetConfigHashPResponse() {
    clusterConfigHash_ = "";
    pathConfigHash_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetConfigHashPResponse(
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
          case 10: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000001;
            clusterConfigHash_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            pathConfigHash_ = bs;
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
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_GetConfigHashPResponse_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_GetConfigHashPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.GetConfigHashPResponse.class, alluxio.grpc.GetConfigHashPResponse.Builder.class);
  }

  private int bitField0_;
  public static final int CLUSTERCONFIGHASH_FIELD_NUMBER = 1;
  private volatile java.lang.Object clusterConfigHash_;
  /**
   * <code>optional string clusterConfigHash = 1;</code>
   */
  public boolean hasClusterConfigHash() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string clusterConfigHash = 1;</code>
   */
  public java.lang.String getClusterConfigHash() {
    java.lang.Object ref = clusterConfigHash_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        clusterConfigHash_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string clusterConfigHash = 1;</code>
   */
  public com.google.protobuf.ByteString
      getClusterConfigHashBytes() {
    java.lang.Object ref = clusterConfigHash_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      clusterConfigHash_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PATHCONFIGHASH_FIELD_NUMBER = 2;
  private volatile java.lang.Object pathConfigHash_;
  /**
   * <code>optional string pathConfigHash = 2;</code>
   */
  public boolean hasPathConfigHash() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string pathConfigHash = 2;</code>
   */
  public java.lang.String getPathConfigHash() {
    java.lang.Object ref = pathConfigHash_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        pathConfigHash_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string pathConfigHash = 2;</code>
   */
  public com.google.protobuf.ByteString
      getPathConfigHashBytes() {
    java.lang.Object ref = pathConfigHash_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      pathConfigHash_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, clusterConfigHash_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, pathConfigHash_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, clusterConfigHash_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, pathConfigHash_);
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
    if (!(obj instanceof alluxio.grpc.GetConfigHashPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.GetConfigHashPResponse other = (alluxio.grpc.GetConfigHashPResponse) obj;

    boolean result = true;
    result = result && (hasClusterConfigHash() == other.hasClusterConfigHash());
    if (hasClusterConfigHash()) {
      result = result && getClusterConfigHash()
          .equals(other.getClusterConfigHash());
    }
    result = result && (hasPathConfigHash() == other.hasPathConfigHash());
    if (hasPathConfigHash()) {
      result = result && getPathConfigHash()
          .equals(other.getPathConfigHash());
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
    if (hasClusterConfigHash()) {
      hash = (37 * hash) + CLUSTERCONFIGHASH_FIELD_NUMBER;
      hash = (53 * hash) + getClusterConfigHash().hashCode();
    }
    if (hasPathConfigHash()) {
      hash = (37 * hash) + PATHCONFIGHASH_FIELD_NUMBER;
      hash = (53 * hash) + getPathConfigHash().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetConfigHashPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.GetConfigHashPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.meta.GetConfigHashPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.meta.GetConfigHashPResponse)
      alluxio.grpc.GetConfigHashPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_GetConfigHashPResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_GetConfigHashPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.GetConfigHashPResponse.class, alluxio.grpc.GetConfigHashPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.GetConfigHashPResponse.newBuilder()
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
      clusterConfigHash_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      pathConfigHash_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_GetConfigHashPResponse_descriptor;
    }

    public alluxio.grpc.GetConfigHashPResponse getDefaultInstanceForType() {
      return alluxio.grpc.GetConfigHashPResponse.getDefaultInstance();
    }

    public alluxio.grpc.GetConfigHashPResponse build() {
      alluxio.grpc.GetConfigHashPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.GetConfigHashPResponse buildPartial() {
      alluxio.grpc.GetConfigHashPResponse result = new alluxio.grpc.GetConfigHashPResponse(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.clusterConfigHash_ = clusterConfigHash_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.pathConfigHash_ = pathConfigHash_;
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
      if (other instanceof alluxio.grpc.GetConfigHashPResponse) {
        return mergeFrom((alluxio.grpc.GetConfigHashPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.GetConfigHashPResponse other) {
      if (other == alluxio.grpc.GetConfigHashPResponse.getDefaultInstance()) return this;
      if (other.hasClusterConfigHash()) {
        bitField0_ |= 0x00000001;
        clusterConfigHash_ = other.clusterConfigHash_;
        onChanged();
      }
      if (other.hasPathConfigHash()) {
        bitField0_ |= 0x00000002;
        pathConfigHash_ = other.pathConfigHash_;
        onChanged();
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
      alluxio.grpc.GetConfigHashPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.GetConfigHashPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object clusterConfigHash_ = "";
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public boolean hasClusterConfigHash() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public java.lang.String getClusterConfigHash() {
      java.lang.Object ref = clusterConfigHash_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          clusterConfigHash_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public com.google.protobuf.ByteString
        getClusterConfigHashBytes() {
      java.lang.Object ref = clusterConfigHash_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        clusterConfigHash_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public Builder setClusterConfigHash(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      clusterConfigHash_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public Builder clearClusterConfigHash() {
      bitField0_ = (bitField0_ & ~0x00000001);
      clusterConfigHash_ = getDefaultInstance().getClusterConfigHash();
      onChanged();
      return this;
    }
    /**
     * <code>optional string clusterConfigHash = 1;</code>
     */
    public Builder setClusterConfigHashBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      clusterConfigHash_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object pathConfigHash_ = "";
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public boolean hasPathConfigHash() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public java.lang.String getPathConfigHash() {
      java.lang.Object ref = pathConfigHash_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          pathConfigHash_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public com.google.protobuf.ByteString
        getPathConfigHashBytes() {
      java.lang.Object ref = pathConfigHash_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        pathConfigHash_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public Builder setPathConfigHash(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      pathConfigHash_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public Builder clearPathConfigHash() {
      bitField0_ = (bitField0_ & ~0x00000002);
      pathConfigHash_ = getDefaultInstance().getPathConfigHash();
      onChanged();
      return this;
    }
    /**
     * <code>optional string pathConfigHash = 2;</code>
     */
    public Builder setPathConfigHashBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      pathConfigHash_ = value;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.meta.GetConfigHashPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.meta.GetConfigHashPResponse)
  private static final alluxio.grpc.GetConfigHashPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.GetConfigHashPResponse();
  }

  public static alluxio.grpc.GetConfigHashPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetConfigHashPResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetConfigHashPResponse>() {
    public GetConfigHashPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetConfigHashPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetConfigHashPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetConfigHashPResponse> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.GetConfigHashPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

