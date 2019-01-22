// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.PAclEntry}
 */
public  final class PAclEntry extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.PAclEntry)
    PAclEntryOrBuilder {
private static final long serialVersionUID = 0L;
  // Use PAclEntry.newBuilder() to construct.
  private PAclEntry(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private PAclEntry() {
    type_ = 0;
    subject_ = "";
    actions_ = java.util.Collections.emptyList();
    isDefault_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private PAclEntry(
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
            int rawValue = input.readEnum();
            alluxio.grpc.PAclEntryType value = alluxio.grpc.PAclEntryType.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(1, rawValue);
            } else {
              bitField0_ |= 0x00000001;
              type_ = rawValue;
            }
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            subject_ = bs;
            break;
          }
          case 24: {
            int rawValue = input.readEnum();
            alluxio.grpc.PAclAction value = alluxio.grpc.PAclAction.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(3, rawValue);
            } else {
              if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
                actions_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000004;
              }
              actions_.add(rawValue);
            }
            break;
          }
          case 26: {
            int length = input.readRawVarint32();
            int oldLimit = input.pushLimit(length);
            while(input.getBytesUntilLimit() > 0) {
              int rawValue = input.readEnum();
              alluxio.grpc.PAclAction value = alluxio.grpc.PAclAction.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(3, rawValue);
              } else {
                if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
                  actions_ = new java.util.ArrayList<java.lang.Integer>();
                  mutable_bitField0_ |= 0x00000004;
                }
                actions_.add(rawValue);
              }
            }
            input.popLimit(oldLimit);
            break;
          }
          case 32: {
            bitField0_ |= 0x00000004;
            isDefault_ = input.readBool();
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
      if (((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
        actions_ = java.util.Collections.unmodifiableList(actions_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_PAclEntry_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_PAclEntry_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.PAclEntry.class, alluxio.grpc.PAclEntry.Builder.class);
  }

  private int bitField0_;
  public static final int TYPE_FIELD_NUMBER = 1;
  private int type_;
  /**
   * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
   */
  public boolean hasType() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
   */
  public alluxio.grpc.PAclEntryType getType() {
    alluxio.grpc.PAclEntryType result = alluxio.grpc.PAclEntryType.valueOf(type_);
    return result == null ? alluxio.grpc.PAclEntryType.Owner : result;
  }

  public static final int SUBJECT_FIELD_NUMBER = 2;
  private volatile java.lang.Object subject_;
  /**
   * <code>optional string subject = 2;</code>
   */
  public boolean hasSubject() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string subject = 2;</code>
   */
  public java.lang.String getSubject() {
    java.lang.Object ref = subject_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        subject_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string subject = 2;</code>
   */
  public com.google.protobuf.ByteString
      getSubjectBytes() {
    java.lang.Object ref = subject_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      subject_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ACTIONS_FIELD_NUMBER = 3;
  private java.util.List<java.lang.Integer> actions_;
  private static final com.google.protobuf.Internal.ListAdapter.Converter<
      java.lang.Integer, alluxio.grpc.PAclAction> actions_converter_ =
          new com.google.protobuf.Internal.ListAdapter.Converter<
              java.lang.Integer, alluxio.grpc.PAclAction>() {
            public alluxio.grpc.PAclAction convert(java.lang.Integer from) {
              alluxio.grpc.PAclAction result = alluxio.grpc.PAclAction.valueOf(from);
              return result == null ? alluxio.grpc.PAclAction.Read : result;
            }
          };
  /**
   * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
   */
  public java.util.List<alluxio.grpc.PAclAction> getActionsList() {
    return new com.google.protobuf.Internal.ListAdapter<
        java.lang.Integer, alluxio.grpc.PAclAction>(actions_, actions_converter_);
  }
  /**
   * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
   */
  public int getActionsCount() {
    return actions_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
   */
  public alluxio.grpc.PAclAction getActions(int index) {
    return actions_converter_.convert(actions_.get(index));
  }

  public static final int ISDEFAULT_FIELD_NUMBER = 4;
  private boolean isDefault_;
  /**
   * <code>optional bool isDefault = 4;</code>
   */
  public boolean hasIsDefault() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional bool isDefault = 4;</code>
   */
  public boolean getIsDefault() {
    return isDefault_;
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
      output.writeEnum(1, type_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, subject_);
    }
    for (int i = 0; i < actions_.size(); i++) {
      output.writeEnum(3, actions_.get(i));
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeBool(4, isDefault_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(1, type_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, subject_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < actions_.size(); i++) {
        dataSize += com.google.protobuf.CodedOutputStream
          .computeEnumSizeNoTag(actions_.get(i));
      }
      size += dataSize;
      size += 1 * actions_.size();
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(4, isDefault_);
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
    if (!(obj instanceof alluxio.grpc.PAclEntry)) {
      return super.equals(obj);
    }
    alluxio.grpc.PAclEntry other = (alluxio.grpc.PAclEntry) obj;

    boolean result = true;
    result = result && (hasType() == other.hasType());
    if (hasType()) {
      result = result && type_ == other.type_;
    }
    result = result && (hasSubject() == other.hasSubject());
    if (hasSubject()) {
      result = result && getSubject()
          .equals(other.getSubject());
    }
    result = result && actions_.equals(other.actions_);
    result = result && (hasIsDefault() == other.hasIsDefault());
    if (hasIsDefault()) {
      result = result && (getIsDefault()
          == other.getIsDefault());
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
    if (hasType()) {
      hash = (37 * hash) + TYPE_FIELD_NUMBER;
      hash = (53 * hash) + type_;
    }
    if (hasSubject()) {
      hash = (37 * hash) + SUBJECT_FIELD_NUMBER;
      hash = (53 * hash) + getSubject().hashCode();
    }
    if (getActionsCount() > 0) {
      hash = (37 * hash) + ACTIONS_FIELD_NUMBER;
      hash = (53 * hash) + actions_.hashCode();
    }
    if (hasIsDefault()) {
      hash = (37 * hash) + ISDEFAULT_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getIsDefault());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.PAclEntry parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PAclEntry parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PAclEntry parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.PAclEntry parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PAclEntry parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PAclEntry parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.PAclEntry prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.PAclEntry}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.PAclEntry)
      alluxio.grpc.PAclEntryOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_PAclEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_PAclEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.PAclEntry.class, alluxio.grpc.PAclEntry.Builder.class);
    }

    // Construct using alluxio.grpc.PAclEntry.newBuilder()
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
      type_ = 0;
      bitField0_ = (bitField0_ & ~0x00000001);
      subject_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      actions_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000004);
      isDefault_ = false;
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_PAclEntry_descriptor;
    }

    public alluxio.grpc.PAclEntry getDefaultInstanceForType() {
      return alluxio.grpc.PAclEntry.getDefaultInstance();
    }

    public alluxio.grpc.PAclEntry build() {
      alluxio.grpc.PAclEntry result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.PAclEntry buildPartial() {
      alluxio.grpc.PAclEntry result = new alluxio.grpc.PAclEntry(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.type_ = type_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.subject_ = subject_;
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        actions_ = java.util.Collections.unmodifiableList(actions_);
        bitField0_ = (bitField0_ & ~0x00000004);
      }
      result.actions_ = actions_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000004;
      }
      result.isDefault_ = isDefault_;
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
      if (other instanceof alluxio.grpc.PAclEntry) {
        return mergeFrom((alluxio.grpc.PAclEntry)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.PAclEntry other) {
      if (other == alluxio.grpc.PAclEntry.getDefaultInstance()) return this;
      if (other.hasType()) {
        setType(other.getType());
      }
      if (other.hasSubject()) {
        bitField0_ |= 0x00000002;
        subject_ = other.subject_;
        onChanged();
      }
      if (!other.actions_.isEmpty()) {
        if (actions_.isEmpty()) {
          actions_ = other.actions_;
          bitField0_ = (bitField0_ & ~0x00000004);
        } else {
          ensureActionsIsMutable();
          actions_.addAll(other.actions_);
        }
        onChanged();
      }
      if (other.hasIsDefault()) {
        setIsDefault(other.getIsDefault());
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
      alluxio.grpc.PAclEntry parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.PAclEntry) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int type_ = 0;
    /**
     * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
     */
    public alluxio.grpc.PAclEntryType getType() {
      alluxio.grpc.PAclEntryType result = alluxio.grpc.PAclEntryType.valueOf(type_);
      return result == null ? alluxio.grpc.PAclEntryType.Owner : result;
    }
    /**
     * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
     */
    public Builder setType(alluxio.grpc.PAclEntryType value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000001;
      type_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.PAclEntryType type = 1;</code>
     */
    public Builder clearType() {
      bitField0_ = (bitField0_ & ~0x00000001);
      type_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object subject_ = "";
    /**
     * <code>optional string subject = 2;</code>
     */
    public boolean hasSubject() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string subject = 2;</code>
     */
    public java.lang.String getSubject() {
      java.lang.Object ref = subject_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          subject_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string subject = 2;</code>
     */
    public com.google.protobuf.ByteString
        getSubjectBytes() {
      java.lang.Object ref = subject_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        subject_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string subject = 2;</code>
     */
    public Builder setSubject(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      subject_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string subject = 2;</code>
     */
    public Builder clearSubject() {
      bitField0_ = (bitField0_ & ~0x00000002);
      subject_ = getDefaultInstance().getSubject();
      onChanged();
      return this;
    }
    /**
     * <code>optional string subject = 2;</code>
     */
    public Builder setSubjectBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      subject_ = value;
      onChanged();
      return this;
    }

    private java.util.List<java.lang.Integer> actions_ =
      java.util.Collections.emptyList();
    private void ensureActionsIsMutable() {
      if (!((bitField0_ & 0x00000004) == 0x00000004)) {
        actions_ = new java.util.ArrayList<java.lang.Integer>(actions_);
        bitField0_ |= 0x00000004;
      }
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public java.util.List<alluxio.grpc.PAclAction> getActionsList() {
      return new com.google.protobuf.Internal.ListAdapter<
          java.lang.Integer, alluxio.grpc.PAclAction>(actions_, actions_converter_);
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public int getActionsCount() {
      return actions_.size();
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public alluxio.grpc.PAclAction getActions(int index) {
      return actions_converter_.convert(actions_.get(index));
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public Builder setActions(
        int index, alluxio.grpc.PAclAction value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureActionsIsMutable();
      actions_.set(index, value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public Builder addActions(alluxio.grpc.PAclAction value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureActionsIsMutable();
      actions_.add(value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public Builder addAllActions(
        java.lang.Iterable<? extends alluxio.grpc.PAclAction> values) {
      ensureActionsIsMutable();
      for (alluxio.grpc.PAclAction value : values) {
        actions_.add(value.getNumber());
      }
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.file.PAclAction actions = 3;</code>
     */
    public Builder clearActions() {
      actions_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000004);
      onChanged();
      return this;
    }

    private boolean isDefault_ ;
    /**
     * <code>optional bool isDefault = 4;</code>
     */
    public boolean hasIsDefault() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bool isDefault = 4;</code>
     */
    public boolean getIsDefault() {
      return isDefault_;
    }
    /**
     * <code>optional bool isDefault = 4;</code>
     */
    public Builder setIsDefault(boolean value) {
      bitField0_ |= 0x00000008;
      isDefault_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool isDefault = 4;</code>
     */
    public Builder clearIsDefault() {
      bitField0_ = (bitField0_ & ~0x00000008);
      isDefault_ = false;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.PAclEntry)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.PAclEntry)
  private static final alluxio.grpc.PAclEntry DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.PAclEntry();
  }

  public static alluxio.grpc.PAclEntry getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<PAclEntry>
      PARSER = new com.google.protobuf.AbstractParser<PAclEntry>() {
    public PAclEntry parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new PAclEntry(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<PAclEntry> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<PAclEntry> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.PAclEntry getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

