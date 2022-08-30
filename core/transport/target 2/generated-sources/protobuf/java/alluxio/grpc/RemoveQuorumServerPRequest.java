// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/journal_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.journal.RemoveQuorumServerPRequest}
 */
public final class RemoveQuorumServerPRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.journal.RemoveQuorumServerPRequest)
    RemoveQuorumServerPRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RemoveQuorumServerPRequest.newBuilder() to construct.
  private RemoveQuorumServerPRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RemoveQuorumServerPRequest() {
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new RemoveQuorumServerPRequest();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RemoveQuorumServerPRequest(
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
            alluxio.grpc.RemoveQuorumServerPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000001) != 0)) {
              subBuilder = options_.toBuilder();
            }
            options_ = input.readMessage(alluxio.grpc.RemoveQuorumServerPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(options_);
              options_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000001;
            break;
          }
          case 18: {
            alluxio.grpc.NetAddress.Builder subBuilder = null;
            if (((bitField0_ & 0x00000002) != 0)) {
              subBuilder = serverAddress_.toBuilder();
            }
            serverAddress_ = input.readMessage(alluxio.grpc.NetAddress.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(serverAddress_);
              serverAddress_ = subBuilder.buildPartial();
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
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.JournalMasterProto.internal_static_alluxio_grpc_journal_RemoveQuorumServerPRequest_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.JournalMasterProto.internal_static_alluxio_grpc_journal_RemoveQuorumServerPRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RemoveQuorumServerPRequest.class, alluxio.grpc.RemoveQuorumServerPRequest.Builder.class);
  }

  private int bitField0_;
  public static final int OPTIONS_FIELD_NUMBER = 1;
  private alluxio.grpc.RemoveQuorumServerPOptions options_;
  /**
   * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
   * @return Whether the options field is set.
   */
  @java.lang.Override
  public boolean hasOptions() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
   * @return The options.
   */
  @java.lang.Override
  public alluxio.grpc.RemoveQuorumServerPOptions getOptions() {
    return options_ == null ? alluxio.grpc.RemoveQuorumServerPOptions.getDefaultInstance() : options_;
  }
  /**
   * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.RemoveQuorumServerPOptionsOrBuilder getOptionsOrBuilder() {
    return options_ == null ? alluxio.grpc.RemoveQuorumServerPOptions.getDefaultInstance() : options_;
  }

  public static final int SERVERADDRESS_FIELD_NUMBER = 2;
  private alluxio.grpc.NetAddress serverAddress_;
  /**
   * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
   * @return Whether the serverAddress field is set.
   */
  @java.lang.Override
  public boolean hasServerAddress() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
   * @return The serverAddress.
   */
  @java.lang.Override
  public alluxio.grpc.NetAddress getServerAddress() {
    return serverAddress_ == null ? alluxio.grpc.NetAddress.getDefaultInstance() : serverAddress_;
  }
  /**
   * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
   */
  @java.lang.Override
  public alluxio.grpc.NetAddressOrBuilder getServerAddressOrBuilder() {
    return serverAddress_ == null ? alluxio.grpc.NetAddress.getDefaultInstance() : serverAddress_;
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
      output.writeMessage(1, getOptions());
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      output.writeMessage(2, getServerAddress());
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getOptions());
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getServerAddress());
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
    if (!(obj instanceof alluxio.grpc.RemoveQuorumServerPRequest)) {
      return super.equals(obj);
    }
    alluxio.grpc.RemoveQuorumServerPRequest other = (alluxio.grpc.RemoveQuorumServerPRequest) obj;

    if (hasOptions() != other.hasOptions()) return false;
    if (hasOptions()) {
      if (!getOptions()
          .equals(other.getOptions())) return false;
    }
    if (hasServerAddress() != other.hasServerAddress()) return false;
    if (hasServerAddress()) {
      if (!getServerAddress()
          .equals(other.getServerAddress())) return false;
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
    if (hasOptions()) {
      hash = (37 * hash) + OPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getOptions().hashCode();
    }
    if (hasServerAddress()) {
      hash = (37 * hash) + SERVERADDRESS_FIELD_NUMBER;
      hash = (53 * hash) + getServerAddress().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RemoveQuorumServerPRequest parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.RemoveQuorumServerPRequest prototype) {
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
   * Protobuf type {@code alluxio.grpc.journal.RemoveQuorumServerPRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.journal.RemoveQuorumServerPRequest)
      alluxio.grpc.RemoveQuorumServerPRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.JournalMasterProto.internal_static_alluxio_grpc_journal_RemoveQuorumServerPRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.JournalMasterProto.internal_static_alluxio_grpc_journal_RemoveQuorumServerPRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RemoveQuorumServerPRequest.class, alluxio.grpc.RemoveQuorumServerPRequest.Builder.class);
    }

    // Construct using alluxio.grpc.RemoveQuorumServerPRequest.newBuilder()
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
        getServerAddressFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      if (optionsBuilder_ == null) {
        options_ = null;
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      if (serverAddressBuilder_ == null) {
        serverAddress_ = null;
      } else {
        serverAddressBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.JournalMasterProto.internal_static_alluxio_grpc_journal_RemoveQuorumServerPRequest_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.RemoveQuorumServerPRequest getDefaultInstanceForType() {
      return alluxio.grpc.RemoveQuorumServerPRequest.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.RemoveQuorumServerPRequest build() {
      alluxio.grpc.RemoveQuorumServerPRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.RemoveQuorumServerPRequest buildPartial() {
      alluxio.grpc.RemoveQuorumServerPRequest result = new alluxio.grpc.RemoveQuorumServerPRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        if (optionsBuilder_ == null) {
          result.options_ = options_;
        } else {
          result.options_ = optionsBuilder_.build();
        }
        to_bitField0_ |= 0x00000001;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        if (serverAddressBuilder_ == null) {
          result.serverAddress_ = serverAddress_;
        } else {
          result.serverAddress_ = serverAddressBuilder_.build();
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
      if (other instanceof alluxio.grpc.RemoveQuorumServerPRequest) {
        return mergeFrom((alluxio.grpc.RemoveQuorumServerPRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RemoveQuorumServerPRequest other) {
      if (other == alluxio.grpc.RemoveQuorumServerPRequest.getDefaultInstance()) return this;
      if (other.hasOptions()) {
        mergeOptions(other.getOptions());
      }
      if (other.hasServerAddress()) {
        mergeServerAddress(other.getServerAddress());
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
      alluxio.grpc.RemoveQuorumServerPRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RemoveQuorumServerPRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private alluxio.grpc.RemoveQuorumServerPOptions options_;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemoveQuorumServerPOptions, alluxio.grpc.RemoveQuorumServerPOptions.Builder, alluxio.grpc.RemoveQuorumServerPOptionsOrBuilder> optionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     * @return Whether the options field is set.
     */
    public boolean hasOptions() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     * @return The options.
     */
    public alluxio.grpc.RemoveQuorumServerPOptions getOptions() {
      if (optionsBuilder_ == null) {
        return options_ == null ? alluxio.grpc.RemoveQuorumServerPOptions.getDefaultInstance() : options_;
      } else {
        return optionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public Builder setOptions(alluxio.grpc.RemoveQuorumServerPOptions value) {
      if (optionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        options_ = value;
        onChanged();
      } else {
        optionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public Builder setOptions(
        alluxio.grpc.RemoveQuorumServerPOptions.Builder builderForValue) {
      if (optionsBuilder_ == null) {
        options_ = builderForValue.build();
        onChanged();
      } else {
        optionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public Builder mergeOptions(alluxio.grpc.RemoveQuorumServerPOptions value) {
      if (optionsBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0) &&
            options_ != null &&
            options_ != alluxio.grpc.RemoveQuorumServerPOptions.getDefaultInstance()) {
          options_ =
            alluxio.grpc.RemoveQuorumServerPOptions.newBuilder(options_).mergeFrom(value).buildPartial();
        } else {
          options_ = value;
        }
        onChanged();
      } else {
        optionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public Builder clearOptions() {
      if (optionsBuilder_ == null) {
        options_ = null;
        onChanged();
      } else {
        optionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public alluxio.grpc.RemoveQuorumServerPOptions.Builder getOptionsBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    public alluxio.grpc.RemoveQuorumServerPOptionsOrBuilder getOptionsOrBuilder() {
      if (optionsBuilder_ != null) {
        return optionsBuilder_.getMessageOrBuilder();
      } else {
        return options_ == null ?
            alluxio.grpc.RemoveQuorumServerPOptions.getDefaultInstance() : options_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.journal.RemoveQuorumServerPOptions options = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.RemoveQuorumServerPOptions, alluxio.grpc.RemoveQuorumServerPOptions.Builder, alluxio.grpc.RemoveQuorumServerPOptionsOrBuilder> 
        getOptionsFieldBuilder() {
      if (optionsBuilder_ == null) {
        optionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.RemoveQuorumServerPOptions, alluxio.grpc.RemoveQuorumServerPOptions.Builder, alluxio.grpc.RemoveQuorumServerPOptionsOrBuilder>(
                getOptions(),
                getParentForChildren(),
                isClean());
        options_ = null;
      }
      return optionsBuilder_;
    }

    private alluxio.grpc.NetAddress serverAddress_;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.NetAddress, alluxio.grpc.NetAddress.Builder, alluxio.grpc.NetAddressOrBuilder> serverAddressBuilder_;
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     * @return Whether the serverAddress field is set.
     */
    public boolean hasServerAddress() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     * @return The serverAddress.
     */
    public alluxio.grpc.NetAddress getServerAddress() {
      if (serverAddressBuilder_ == null) {
        return serverAddress_ == null ? alluxio.grpc.NetAddress.getDefaultInstance() : serverAddress_;
      } else {
        return serverAddressBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public Builder setServerAddress(alluxio.grpc.NetAddress value) {
      if (serverAddressBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        serverAddress_ = value;
        onChanged();
      } else {
        serverAddressBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public Builder setServerAddress(
        alluxio.grpc.NetAddress.Builder builderForValue) {
      if (serverAddressBuilder_ == null) {
        serverAddress_ = builderForValue.build();
        onChanged();
      } else {
        serverAddressBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public Builder mergeServerAddress(alluxio.grpc.NetAddress value) {
      if (serverAddressBuilder_ == null) {
        if (((bitField0_ & 0x00000002) != 0) &&
            serverAddress_ != null &&
            serverAddress_ != alluxio.grpc.NetAddress.getDefaultInstance()) {
          serverAddress_ =
            alluxio.grpc.NetAddress.newBuilder(serverAddress_).mergeFrom(value).buildPartial();
        } else {
          serverAddress_ = value;
        }
        onChanged();
      } else {
        serverAddressBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000002;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public Builder clearServerAddress() {
      if (serverAddressBuilder_ == null) {
        serverAddress_ = null;
        onChanged();
      } else {
        serverAddressBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public alluxio.grpc.NetAddress.Builder getServerAddressBuilder() {
      bitField0_ |= 0x00000002;
      onChanged();
      return getServerAddressFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    public alluxio.grpc.NetAddressOrBuilder getServerAddressOrBuilder() {
      if (serverAddressBuilder_ != null) {
        return serverAddressBuilder_.getMessageOrBuilder();
      } else {
        return serverAddress_ == null ?
            alluxio.grpc.NetAddress.getDefaultInstance() : serverAddress_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.NetAddress serverAddress = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.NetAddress, alluxio.grpc.NetAddress.Builder, alluxio.grpc.NetAddressOrBuilder> 
        getServerAddressFieldBuilder() {
      if (serverAddressBuilder_ == null) {
        serverAddressBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.NetAddress, alluxio.grpc.NetAddress.Builder, alluxio.grpc.NetAddressOrBuilder>(
                getServerAddress(),
                getParentForChildren(),
                isClean());
        serverAddress_ = null;
      }
      return serverAddressBuilder_;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.journal.RemoveQuorumServerPRequest)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.journal.RemoveQuorumServerPRequest)
  private static final alluxio.grpc.RemoveQuorumServerPRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RemoveQuorumServerPRequest();
  }

  public static alluxio.grpc.RemoveQuorumServerPRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RemoveQuorumServerPRequest>
      PARSER = new com.google.protobuf.AbstractParser<RemoveQuorumServerPRequest>() {
    @java.lang.Override
    public RemoveQuorumServerPRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RemoveQuorumServerPRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RemoveQuorumServerPRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RemoveQuorumServerPRequest> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.RemoveQuorumServerPRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

