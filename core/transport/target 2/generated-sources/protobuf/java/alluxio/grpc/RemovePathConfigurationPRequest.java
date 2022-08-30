// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.meta.RemovePathConfigurationPRequest}
 */
public final class RemovePathConfigurationPRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.meta.RemovePathConfigurationPRequest)
    RemovePathConfigurationPRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RemovePathConfigurationPRequest.newBuilder() to construct.
  private RemovePathConfigurationPRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RemovePathConfigurationPRequest() {
    path_ = "";
    keys_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new RemovePathConfigurationPRequest();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RemovePathConfigurationPRequest(
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
          case 10: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000001;
            path_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            if (!((mutable_bitField0_ & 0x00000002) != 0)) {
              keys_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000002;
            }
            keys_.add(bs);
            break;
          }
          case 26: {
            alluxio.grpc.RemovePathConfigurationPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) != 0)) {
              subBuilder = options_.toBuilder();
            }
            options_ = input.readMessage(alluxio.grpc.RemovePathConfigurationPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(options_);
              options_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000002;
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
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
      if (((mutable_bitField0_ & 0x00000002) != 0)) {
        keys_ = keys_.getUnmodifiableView();
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_RemovePathConfigurationPRequest_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_RemovePathConfigurationPRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RemovePathConfigurationPRequest.class, alluxio.grpc.RemovePathConfigurationPRequest.Builder.class);
  }

  private int bitField0_;
  public static final int PATH_FIELD_NUMBER = 1;
  private volatile java.lang.Object path_;
  /**
   * <code>optional string path = 1;</code>
   * @return Whether the path field is set.
   */
  @java.lang.Override
  public boolean hasPath() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional string path = 1;</code>
   * @return The path.
   */
  @java.lang.Override
  public java.lang.String getPath() {
    java.lang.Object ref = path_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        path_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string path = 1;</code>
   * @return The bytes for path.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getPathBytes() {
    java.lang.Object ref = path_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      path_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int KEYS_FIELD_NUMBER = 2;
  private com.google.protobuf.LazyStringList keys_;
  /**
   * <code>repeated string keys = 2;</code>
   * @return A list containing the keys.
   */
  public com.google.protobuf.ProtocolStringList
      getKeysList() {
    return keys_;
  }
  /**
   * <code>repeated string keys = 2;</code>
   * @return The count of keys.
   */
  public int getKeysCount() {
    return keys_.size();
  }
  /**
   * <code>repeated string keys = 2;</code>
   * @param index The index of the element to return.
   * @return The keys at the given index.
   */
  public java.lang.String getKeys(int index) {
    return keys_.get(index);
  }
  /**
   * <code>repeated string keys = 2;</code>
   * @param index The index of the value to return.
   * @return The bytes of the keys at the given index.
   */
  public com.google.protobuf.ByteString
      getKeysBytes(int index) {
    return keys_.getByteString(index);
  }

  public static final int OPTIONS_FIELD_NUMBER = 3;
  private alluxio.grpc.RemovePathConfigurationPOptions options_;
  /**
   * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
   * @return Whether the options field is set.
   */
  @java.lang.Override
  public boolean hasOptions() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
   * @return The options.
   */
  @java.lang.Override
  public alluxio.grpc.RemovePathConfigurationPOptions getOptions() {
    return options_ == null ? alluxio.grpc.RemovePathConfigurationPOptions.getDefaultInstance() : options_;
  }
  /**
   * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
   */
  @java.lang.Override
  public alluxio.grpc.RemovePathConfigurationPOptionsOrBuilder getOptionsOrBuilder() {
    return options_ == null ? alluxio.grpc.RemovePathConfigurationPOptions.getDefaultInstance() : options_;
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, path_);
    }
    for (int i = 0; i < keys_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, keys_.getRaw(i));
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      output.writeMessage(3, getOptions());
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, path_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < keys_.size(); i++) {
        dataSize += computeStringSizeNoTag(keys_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getKeysList().size();
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, getOptions());
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
    if (!(obj instanceof alluxio.grpc.RemovePathConfigurationPRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.RemovePathConfigurationPRequest other = (alluxio.grpc.RemovePathConfigurationPRequest) obj;

    if (hasPath() != other.hasPath()) return false;
    if (hasPath()) {
      if (!getPath()
          .equals(other.getPath())) return false;
    }
    if (!getKeysList()
        .equals(other.getKeysList())) return false;
    if (hasOptions() != other.hasOptions()) return false;
    if (hasOptions()) {
      if (!getOptions()
          .equals(other.getOptions())) return false;
    }
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasPath()) {
      hash = (37 * hash) + PATH_FIELD_NUMBER;
      hash = (53 * hash) + getPath().hashCode();
    }
    if (getKeysCount() > 0) {
      hash = (37 * hash) + KEYS_FIELD_NUMBER;
      hash = (53 * hash) + getKeysList().hashCode();
    }
    if (hasOptions()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOptions().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemovePathConfigurationPRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(alluxio.grpc.RemovePathConfigurationPRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
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
   * Protobuf type {@code alluxio.grpc.meta.RemovePathConfigurationPRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.meta.RemovePathConfigurationPRequest)
      alluxio.grpc.RemovePathConfigurationPRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_RemovePathConfigurationPRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_RemovePathConfigurationPRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RemovePathConfigurationPRequest.class, alluxio.grpc.RemovePathConfigurationPRequest.Builder.class);
    }

    // Construct using alluxio.grpc.RemovePathConfigurationPRequest.newBuilder()
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
        getOptionsFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      path_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      keys_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000002);
      if (optionsBuilder_ == null) {
        options_ = null;
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_RemovePathConfigurationPRequest_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.RemovePathConfigurationPRequest getDefaultInstanceForType() {
      return alluxio.grpc.RemovePathConfigurationPRequest.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.RemovePathConfigurationPRequest build() {
      alluxio.grpc.RemovePathConfigurationPRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.RemovePathConfigurationPRequest buildPartial() {
      alluxio.grpc.RemovePathConfigurationPRequest result = new alluxio.grpc.RemovePathConfigurationPRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        to_bitField0_ |= 0x00000001;
      }
      result.path_ = path_;
      if (((bitField0_ & 0x00000002) != 0)) {
        keys_ = keys_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000002);
      }
      result.keys_ = keys_;
      if (((from_bitField0_ & 0x00000004) != 0)) {
        if (optionsBuilder_ == null) {
          result.options_ = options_;
        } else {
          result.options_ = optionsBuilder_.build();
        }
        to_bitField0_ |= 0x00000002;
      }
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof alluxio.grpc.RemovePathConfigurationPRequest) {
        return mergeFrom((alluxio.grpc.RemovePathConfigurationPRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RemovePathConfigurationPRequest other) {
      if (other == alluxio.grpc.RemovePathConfigurationPRequest.getDefaultInstance()) return this;
      if (other.hasPath()) {
        bitField0_ |= 0x00000001;
        path_ = other.path_;
        onChanged();
      }
      if (!other.keys_.isEmpty()) {
        if (keys_.isEmpty()) {
          keys_ = other.keys_;
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          ensureKeysIsMutable();
          keys_.addAll(other.keys_);
        }
        onChanged();
      }
      if (other.hasOptions()) {
        mergeOptions(other.getOptions());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.RemovePathConfigurationPRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RemovePathConfigurationPRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object path_ = "";
    /**
     * <code>optional string path = 1;</code>
     * @return Whether the path field is set.
     */
    public boolean hasPath() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string path = 1;</code>
     * @return The path.
     */
    public java.lang.String getPath() {
      java.lang.Object ref = path_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          path_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string path = 1;</code>
     * @return The bytes for path.
     */
    public com.google.protobuf.ByteString
        getPathBytes() {
      java.lang.Object ref = path_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        path_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string path = 1;</code>
     * @param value The path to set.
     * @return This builder for chaining.
     */
    public Builder setPath(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      path_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string path = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearPath() {
      bitField0_ = (bitField0_ & ~0x00000001);
      path_ = getDefaultInstance().getPath();
      onChanged();
      return this;
    }
    /**
     * <code>optional string path = 1;</code>
     * @param value The bytes for path to set.
     * @return This builder for chaining.
     */
    public Builder setPathBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      path_ = value;
      onChanged();
      return this;
    }

    private com.google.protobuf.LazyStringList keys_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureKeysIsMutable() {
      if (!((bitField0_ & 0x00000002) != 0)) {
        keys_ = new com.google.protobuf.LazyStringArrayList(keys_);
        bitField0_ |= 0x00000002;
       }
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @return A list containing the keys.
     */
    public com.google.protobuf.ProtocolStringList
        getKeysList() {
      return keys_.getUnmodifiableView();
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @return The count of keys.
     */
    public int getKeysCount() {
      return keys_.size();
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    public java.lang.String getKeys(int index) {
      return keys_.get(index);
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param index The index of the value to return.
     * @return The bytes of the keys at the given index.
     */
    public com.google.protobuf.ByteString
        getKeysBytes(int index) {
      return keys_.getByteString(index);
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param index The index to set the value at.
     * @param value The keys to set.
     * @return This builder for chaining.
     */
    public Builder setKeys(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureKeysIsMutable();
      keys_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param value The keys to add.
     * @return This builder for chaining.
     */
    public Builder addKeys(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureKeysIsMutable();
      keys_.add(value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param values The keys to add.
     * @return This builder for chaining.
     */
    public Builder addAllKeys(
        java.lang.Iterable<java.lang.String> values) {
      ensureKeysIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, keys_);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearKeys() {
      keys_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string keys = 2;</code>
     * @param value The bytes of the keys to add.
     * @return This builder for chaining.
     */
    public Builder addKeysBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureKeysIsMutable();
      keys_.add(value);
      onChanged();
      return this;
    }

    private alluxio.grpc.RemovePathConfigurationPOptions options_;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemovePathConfigurationPOptions, alluxio.grpc.RemovePathConfigurationPOptions.Builder, alluxio.grpc.RemovePathConfigurationPOptionsOrBuilder> optionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     * @return Whether the options field is set.
     */
    public boolean hasOptions() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     * @return The options.
     */
    public alluxio.grpc.RemovePathConfigurationPOptions getOptions() {
      if (optionsBuilder_ == null) {
        return options_ == null ? alluxio.grpc.RemovePathConfigurationPOptions.getDefaultInstance() : options_;
      } else {
        return optionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public Builder setOptions(alluxio.grpc.RemovePathConfigurationPOptions value) {
      if (optionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        options_ = value;
        onChanged();
      } else {
        optionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public Builder setOptions(
        alluxio.grpc.RemovePathConfigurationPOptions.Builder builderForValue) {
      if (optionsBuilder_ == null) {
        options_ = builderForValue.build();
        onChanged();
      } else {
        optionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public Builder mergeOptions(alluxio.grpc.RemovePathConfigurationPOptions value) {
      if (optionsBuilder_ == null) {
        if (((bitField0_ & 0x00000004) != 0) &&
            options_ != null &&
            options_ != alluxio.grpc.RemovePathConfigurationPOptions.getDefaultInstance()) {
          options_ =
            alluxio.grpc.RemovePathConfigurationPOptions.newBuilder(options_).mergeFrom(value).buildPartial();
        } else {
          options_ = value;
        }
        onChanged();
      } else {
        optionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public Builder clearOptions() {
      if (optionsBuilder_ == null) {
        options_ = null;
        onChanged();
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public alluxio.grpc.RemovePathConfigurationPOptions.Builder getOptionsBuilder() {
      bitField0_ |= 0x00000004;
      onChanged();
      return getOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    public alluxio.grpc.RemovePathConfigurationPOptionsOrBuilder getOptionsOrBuilder() {
      if (optionsBuilder_ != null) {
        return optionsBuilder_.getMessageOrBuilder();
      } else {
        return options_ == null ?
            alluxio.grpc.RemovePathConfigurationPOptions.getDefaultInstance() : options_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.meta.RemovePathConfigurationPOptions options = 3;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemovePathConfigurationPOptions, alluxio.grpc.RemovePathConfigurationPOptions.Builder, alluxio.grpc.RemovePathConfigurationPOptionsOrBuilder> 
        getOptionsFieldBuilder() {
      if (optionsBuilder_ == null) {
        optionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.RemovePathConfigurationPOptions, alluxio.grpc.RemovePathConfigurationPOptions.Builder, alluxio.grpc.RemovePathConfigurationPOptionsOrBuilder>(
                getOptions(),
                getParentForChildren(),
                isClean());
        options_ = null;
      }
      return optionsBuilder_;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.meta.RemovePathConfigurationPRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.meta.RemovePathConfigurationPRequest)
  private static final alluxio.grpc.RemovePathConfigurationPRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RemovePathConfigurationPRequest();
  }

  public static alluxio.grpc.RemovePathConfigurationPRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RemovePathConfigurationPRequest>
      PARSER = new com.google.protobuf.AbstractParser<RemovePathConfigurationPRequest>() {
    @java.lang.Override
    public RemovePathConfigurationPRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RemovePathConfigurationPRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RemovePathConfigurationPRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RemovePathConfigurationPRequest> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.RemovePathConfigurationPRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

