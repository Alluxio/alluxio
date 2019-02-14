// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.UfsInfo}
 */
public  final class UfsInfo extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.UfsInfo)
    UfsInfoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use UfsInfo.newBuilder() to construct.
  private UfsInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private UfsInfo() {
    uri_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private UfsInfo(
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
            uri_ = bs;
            break;
          }
          case 18: {
            alluxio.grpc.MountPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
              subBuilder = properties_.toBuilder();
            }
            properties_ = input.readMessage(alluxio.grpc.MountPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(properties_);
              properties_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000002;
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_UfsInfo_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_UfsInfo_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.UfsInfo.class, alluxio.grpc.UfsInfo.Builder.class);
  }

  private int bitField0_;
  public static final int URI_FIELD_NUMBER = 1;
  private volatile java.lang.Object uri_;
  /**
   * <code>optional string uri = 1;</code>
   */
  public boolean hasUri() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string uri = 1;</code>
   */
  public java.lang.String getUri() {
    java.lang.Object ref = uri_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        uri_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string uri = 1;</code>
   */
  public com.google.protobuf.ByteString
      getUriBytes() {
    java.lang.Object ref = uri_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      uri_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PROPERTIES_FIELD_NUMBER = 2;
  private alluxio.grpc.MountPOptions properties_;
  /**
   * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
   */
  public boolean hasProperties() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
   */
  public alluxio.grpc.MountPOptions getProperties() {
    return properties_ == null ? alluxio.grpc.MountPOptions.getDefaultInstance() : properties_;
  }
  /**
   * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
   */
  public alluxio.grpc.MountPOptionsOrBuilder getPropertiesOrBuilder() {
    return properties_ == null ? alluxio.grpc.MountPOptions.getDefaultInstance() : properties_;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, uri_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeMessage(2, getProperties());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, uri_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getProperties());
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
    if (!(obj instanceof alluxio.grpc.UfsInfo)) {
      return super.equals(obj);
    }
    alluxio.grpc.UfsInfo other = (alluxio.grpc.UfsInfo) obj;

    boolean result = true;
    result = result && (hasUri() == other.hasUri());
    if (hasUri()) {
      result = result && getUri()
          .equals(other.getUri());
    }
    result = result && (hasProperties() == other.hasProperties());
    if (hasProperties()) {
      result = result && getProperties()
          .equals(other.getProperties());
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
    if (hasUri()) {
      hash = (37 * hash) + URI_FIELD_NUMBER;
      hash = (53 * hash) + getUri().hashCode();
    }
    if (hasProperties()) {
      hash = (37 * hash) + PROPERTIES_FIELD_NUMBER;
      hash = (53 * hash) + getProperties().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.UfsInfo parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.UfsInfo parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.UfsInfo parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.UfsInfo parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.UfsInfo parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.UfsInfo parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.UfsInfo prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.UfsInfo}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.UfsInfo)
      alluxio.grpc.UfsInfoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_UfsInfo_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_UfsInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.UfsInfo.class, alluxio.grpc.UfsInfo.Builder.class);
    }

    // Construct using alluxio.grpc.UfsInfo.newBuilder()
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
        getPropertiesFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      uri_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      if (propertiesBuilder_ == null) {
        properties_ = null;
      } else {
        propertiesBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_UfsInfo_descriptor;
    }

    public alluxio.grpc.UfsInfo getDefaultInstanceForType() {
      return alluxio.grpc.UfsInfo.getDefaultInstance();
    }

    public alluxio.grpc.UfsInfo build() {
      alluxio.grpc.UfsInfo result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.UfsInfo buildPartial() {
      alluxio.grpc.UfsInfo result = new alluxio.grpc.UfsInfo(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.uri_ = uri_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      if (propertiesBuilder_ == null) {
        result.properties_ = properties_;
      } else {
        result.properties_ = propertiesBuilder_.build();
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
      if (other instanceof alluxio.grpc.UfsInfo) {
        return mergeFrom((alluxio.grpc.UfsInfo)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.UfsInfo other) {
      if (other == alluxio.grpc.UfsInfo.getDefaultInstance()) return this;
      if (other.hasUri()) {
        bitField0_ |= 0x00000001;
        uri_ = other.uri_;
        onChanged();
      }
      if (other.hasProperties()) {
        mergeProperties(other.getProperties());
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
      alluxio.grpc.UfsInfo parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.UfsInfo) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object uri_ = "";
    /**
     * <code>optional string uri = 1;</code>
     */
    public boolean hasUri() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string uri = 1;</code>
     */
    public java.lang.String getUri() {
      java.lang.Object ref = uri_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          uri_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string uri = 1;</code>
     */
    public com.google.protobuf.ByteString
        getUriBytes() {
      java.lang.Object ref = uri_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        uri_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string uri = 1;</code>
     */
    public Builder setUri(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      uri_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string uri = 1;</code>
     */
    public Builder clearUri() {
      bitField0_ = (bitField0_ & ~0x00000001);
      uri_ = getDefaultInstance().getUri();
      onChanged();
      return this;
    }
    /**
     * <code>optional string uri = 1;</code>
     */
    public Builder setUriBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      uri_ = value;
      onChanged();
      return this;
    }

    private alluxio.grpc.MountPOptions properties_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.MountPOptions, alluxio.grpc.MountPOptions.Builder, alluxio.grpc.MountPOptionsOrBuilder> propertiesBuilder_;
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public boolean hasProperties() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public alluxio.grpc.MountPOptions getProperties() {
      if (propertiesBuilder_ == null) {
        return properties_ == null ? alluxio.grpc.MountPOptions.getDefaultInstance() : properties_;
      } else {
        return propertiesBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public Builder setProperties(alluxio.grpc.MountPOptions value) {
      if (propertiesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        properties_ = value;
        onChanged();
      } else {
        propertiesBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public Builder setProperties(
        alluxio.grpc.MountPOptions.Builder builderForValue) {
      if (propertiesBuilder_ == null) {
        properties_ = builderForValue.build();
        onChanged();
      } else {
        propertiesBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public Builder mergeProperties(alluxio.grpc.MountPOptions value) {
      if (propertiesBuilder_ == null) {
        if (((bitField0_ & 0x00000002) == 0x00000002) &&
            properties_ != null &&
            properties_ != alluxio.grpc.MountPOptions.getDefaultInstance()) {
          properties_ =
            alluxio.grpc.MountPOptions.newBuilder(properties_).mergeFrom(value).buildPartial();
        } else {
          properties_ = value;
        }
        onChanged();
      } else {
        propertiesBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public Builder clearProperties() {
      if (propertiesBuilder_ == null) {
        properties_ = null;
        onChanged();
      } else {
        propertiesBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public alluxio.grpc.MountPOptions.Builder getPropertiesBuilder() {
      bitField0_ |= 0x00000002;
      onChanged();
      return getPropertiesFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    public alluxio.grpc.MountPOptionsOrBuilder getPropertiesOrBuilder() {
      if (propertiesBuilder_ != null) {
        return propertiesBuilder_.getMessageOrBuilder();
      } else {
        return properties_ == null ?
            alluxio.grpc.MountPOptions.getDefaultInstance() : properties_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.MountPOptions properties = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.MountPOptions, alluxio.grpc.MountPOptions.Builder, alluxio.grpc.MountPOptionsOrBuilder> 
        getPropertiesFieldBuilder() {
      if (propertiesBuilder_ == null) {
        propertiesBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.MountPOptions, alluxio.grpc.MountPOptions.Builder, alluxio.grpc.MountPOptionsOrBuilder>(
                getProperties(),
                getParentForChildren(),
                isClean());
        properties_ = null;
      }
      return propertiesBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.UfsInfo)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.UfsInfo)
  private static final alluxio.grpc.UfsInfo DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.UfsInfo();
  }

  public static alluxio.grpc.UfsInfo getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<UfsInfo>
      PARSER = new com.google.protobuf.AbstractParser<UfsInfo>() {
    public UfsInfo parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new UfsInfo(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<UfsInfo> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<UfsInfo> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.UfsInfo getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

