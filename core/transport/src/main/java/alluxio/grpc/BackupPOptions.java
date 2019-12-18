// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.meta.BackupPOptions}
 */
public  final class BackupPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.meta.BackupPOptions)
    BackupPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use BackupPOptions.newBuilder() to construct.
  private BackupPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private BackupPOptions() {
    localFileSystem_ = false;
    runAsync_ = false;
    allowLeader_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private BackupPOptions(
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
            localFileSystem_ = input.readBool();
            break;
          }
          case 16: {
            bitField0_ |= 0x00000002;
            runAsync_ = input.readBool();
            break;
          }
          case 24: {
            bitField0_ |= 0x00000004;
            allowLeader_ = input.readBool();
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
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_BackupPOptions_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_BackupPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.BackupPOptions.class, alluxio.grpc.BackupPOptions.Builder.class);
  }

  private int bitField0_;
  public static final int LOCALFILESYSTEM_FIELD_NUMBER = 1;
  private boolean localFileSystem_;
  /**
   * <code>optional bool localFileSystem = 1;</code>
   */
  public boolean hasLocalFileSystem() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional bool localFileSystem = 1;</code>
   */
  public boolean getLocalFileSystem() {
    return localFileSystem_;
  }

  public static final int RUNASYNC_FIELD_NUMBER = 2;
  private boolean runAsync_;
  /**
   * <code>optional bool runAsync = 2;</code>
   */
  public boolean hasRunAsync() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional bool runAsync = 2;</code>
   */
  public boolean getRunAsync() {
    return runAsync_;
  }

  public static final int ALLOWLEADER_FIELD_NUMBER = 3;
  private boolean allowLeader_;
  /**
   * <code>optional bool allowLeader = 3;</code>
   */
  public boolean hasAllowLeader() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional bool allowLeader = 3;</code>
   */
  public boolean getAllowLeader() {
    return allowLeader_;
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
      output.writeBool(1, localFileSystem_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeBool(2, runAsync_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeBool(3, allowLeader_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(1, localFileSystem_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(2, runAsync_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(3, allowLeader_);
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
    if (!(obj instanceof alluxio.grpc.BackupPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.BackupPOptions other = (alluxio.grpc.BackupPOptions) obj;

    boolean result = true;
    result = result && (hasLocalFileSystem() == other.hasLocalFileSystem());
    if (hasLocalFileSystem()) {
      result = result && (getLocalFileSystem()
          == other.getLocalFileSystem());
    }
    result = result && (hasRunAsync() == other.hasRunAsync());
    if (hasRunAsync()) {
      result = result && (getRunAsync()
          == other.getRunAsync());
    }
    result = result && (hasAllowLeader() == other.hasAllowLeader());
    if (hasAllowLeader()) {
      result = result && (getAllowLeader()
          == other.getAllowLeader());
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
    if (hasLocalFileSystem()) {
      hash = (37 * hash) + LOCALFILESYSTEM_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getLocalFileSystem());
    }
    if (hasRunAsync()) {
      hash = (37 * hash) + RUNASYNC_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getRunAsync());
    }
    if (hasAllowLeader()) {
      hash = (37 * hash) + ALLOWLEADER_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getAllowLeader());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.BackupPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.BackupPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BackupPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.BackupPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.BackupPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.meta.BackupPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.meta.BackupPOptions)
      alluxio.grpc.BackupPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_BackupPOptions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_BackupPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.BackupPOptions.class, alluxio.grpc.BackupPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.BackupPOptions.newBuilder()
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
      localFileSystem_ = false;
      bitField0_ = (bitField0_ & ~0x00000001);
      runAsync_ = false;
      bitField0_ = (bitField0_ & ~0x00000002);
      allowLeader_ = false;
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_BackupPOptions_descriptor;
    }

    public alluxio.grpc.BackupPOptions getDefaultInstanceForType() {
      return alluxio.grpc.BackupPOptions.getDefaultInstance();
    }

    public alluxio.grpc.BackupPOptions build() {
      alluxio.grpc.BackupPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.BackupPOptions buildPartial() {
      alluxio.grpc.BackupPOptions result = new alluxio.grpc.BackupPOptions(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.localFileSystem_ = localFileSystem_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.runAsync_ = runAsync_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.allowLeader_ = allowLeader_;
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
      if (other instanceof alluxio.grpc.BackupPOptions) {
        return mergeFrom((alluxio.grpc.BackupPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.BackupPOptions other) {
      if (other == alluxio.grpc.BackupPOptions.getDefaultInstance()) return this;
      if (other.hasLocalFileSystem()) {
        setLocalFileSystem(other.getLocalFileSystem());
      }
      if (other.hasRunAsync()) {
        setRunAsync(other.getRunAsync());
      }
      if (other.hasAllowLeader()) {
        setAllowLeader(other.getAllowLeader());
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
      alluxio.grpc.BackupPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.BackupPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private boolean localFileSystem_ ;
    /**
     * <code>optional bool localFileSystem = 1;</code>
     */
    public boolean hasLocalFileSystem() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bool localFileSystem = 1;</code>
     */
    public boolean getLocalFileSystem() {
      return localFileSystem_;
    }
    /**
     * <code>optional bool localFileSystem = 1;</code>
     */
    public Builder setLocalFileSystem(boolean value) {
      bitField0_ |= 0x00000001;
      localFileSystem_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool localFileSystem = 1;</code>
     */
    public Builder clearLocalFileSystem() {
      bitField0_ = (bitField0_ & ~0x00000001);
      localFileSystem_ = false;
      onChanged();
      return this;
    }

    private boolean runAsync_ ;
    /**
     * <code>optional bool runAsync = 2;</code>
     */
    public boolean hasRunAsync() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bool runAsync = 2;</code>
     */
    public boolean getRunAsync() {
      return runAsync_;
    }
    /**
     * <code>optional bool runAsync = 2;</code>
     */
    public Builder setRunAsync(boolean value) {
      bitField0_ |= 0x00000002;
      runAsync_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool runAsync = 2;</code>
     */
    public Builder clearRunAsync() {
      bitField0_ = (bitField0_ & ~0x00000002);
      runAsync_ = false;
      onChanged();
      return this;
    }

    private boolean allowLeader_ ;
    /**
     * <code>optional bool allowLeader = 3;</code>
     */
    public boolean hasAllowLeader() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional bool allowLeader = 3;</code>
     */
    public boolean getAllowLeader() {
      return allowLeader_;
    }
    /**
     * <code>optional bool allowLeader = 3;</code>
     */
    public Builder setAllowLeader(boolean value) {
      bitField0_ |= 0x00000004;
      allowLeader_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool allowLeader = 3;</code>
     */
    public Builder clearAllowLeader() {
      bitField0_ = (bitField0_ & ~0x00000004);
      allowLeader_ = false;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.meta.BackupPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.meta.BackupPOptions)
  private static final alluxio.grpc.BackupPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.BackupPOptions();
  }

  public static alluxio.grpc.BackupPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<BackupPOptions>
      PARSER = new com.google.protobuf.AbstractParser<BackupPOptions>() {
    public BackupPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new BackupPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<BackupPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<BackupPOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.BackupPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

