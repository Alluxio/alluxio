// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: meta_master_options.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.GetMasterInfoPOptions}
 */
public  final class GetMasterInfoPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.GetMasterInfoPOptions)
    GetMasterInfoPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetMasterInfoPOptions.newBuilder() to construct.
  private GetMasterInfoPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetMasterInfoPOptions() {
    filter_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetMasterInfoPOptions(
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
            alluxio.grpc.MasterInfoField value = alluxio.grpc.MasterInfoField.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(1, rawValue);
            } else {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                filter_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000001;
              }
              filter_.add(rawValue);
            }
            break;
          }
          case 10: {
            int length = input.readRawVarint32();
            int oldLimit = input.pushLimit(length);
            while(input.getBytesUntilLimit() > 0) {
              int rawValue = input.readEnum();
              alluxio.grpc.MasterInfoField value = alluxio.grpc.MasterInfoField.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                  filter_ = new java.util.ArrayList<java.lang.Integer>();
                  mutable_bitField0_ |= 0x00000001;
                }
                filter_.add(rawValue);
              }
            }
            input.popLimit(oldLimit);
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
      if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
        filter_ = java.util.Collections.unmodifiableList(filter_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.MetaMasterProtoOptions.internal_static_alluxio_grpc_GetMasterInfoPOptions_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.MetaMasterProtoOptions.internal_static_alluxio_grpc_GetMasterInfoPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.GetMasterInfoPOptions.class, alluxio.grpc.GetMasterInfoPOptions.Builder.class);
  }

  public static final int FILTER_FIELD_NUMBER = 1;
  private java.util.List<java.lang.Integer> filter_;
  private static final com.google.protobuf.Internal.ListAdapter.Converter<
      java.lang.Integer, alluxio.grpc.MasterInfoField> filter_converter_ =
          new com.google.protobuf.Internal.ListAdapter.Converter<
              java.lang.Integer, alluxio.grpc.MasterInfoField>() {
            public alluxio.grpc.MasterInfoField convert(java.lang.Integer from) {
              alluxio.grpc.MasterInfoField result = alluxio.grpc.MasterInfoField.valueOf(from);
              return result == null ? alluxio.grpc.MasterInfoField.LEADER_MASTER_ADDRESS : result;
            }
          };
  /**
   * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
   */
  public java.util.List<alluxio.grpc.MasterInfoField> getFilterList() {
    return new com.google.protobuf.Internal.ListAdapter<
        java.lang.Integer, alluxio.grpc.MasterInfoField>(filter_, filter_converter_);
  }
  /**
   * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
   */
  public int getFilterCount() {
    return filter_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
   */
  public alluxio.grpc.MasterInfoField getFilter(int index) {
    return filter_converter_.convert(filter_.get(index));
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
    for (int i = 0; i < filter_.size(); i++) {
      output.writeEnum(1, filter_.get(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    {
      int dataSize = 0;
      for (int i = 0; i < filter_.size(); i++) {
        dataSize += com.google.protobuf.CodedOutputStream
          .computeEnumSizeNoTag(filter_.get(i));
      }
      size += dataSize;
      size += 1 * filter_.size();
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
    if (!(obj instanceof alluxio.grpc.GetMasterInfoPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.GetMasterInfoPOptions other = (alluxio.grpc.GetMasterInfoPOptions) obj;

    boolean result = true;
    result = result && filter_.equals(other.filter_);
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
    if (getFilterCount() > 0) {
      hash = (37 * hash) + FILTER_FIELD_NUMBER;
      hash = (53 * hash) + filter_.hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetMasterInfoPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.GetMasterInfoPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.GetMasterInfoPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.GetMasterInfoPOptions)
      alluxio.grpc.GetMasterInfoPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.MetaMasterProtoOptions.internal_static_alluxio_grpc_GetMasterInfoPOptions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.MetaMasterProtoOptions.internal_static_alluxio_grpc_GetMasterInfoPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.GetMasterInfoPOptions.class, alluxio.grpc.GetMasterInfoPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.GetMasterInfoPOptions.newBuilder()
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
      filter_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.MetaMasterProtoOptions.internal_static_alluxio_grpc_GetMasterInfoPOptions_descriptor;
    }

    public alluxio.grpc.GetMasterInfoPOptions getDefaultInstanceForType() {
      return alluxio.grpc.GetMasterInfoPOptions.getDefaultInstance();
    }

    public alluxio.grpc.GetMasterInfoPOptions build() {
      alluxio.grpc.GetMasterInfoPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.GetMasterInfoPOptions buildPartial() {
      alluxio.grpc.GetMasterInfoPOptions result = new alluxio.grpc.GetMasterInfoPOptions(this);
      int from_bitField0_ = bitField0_;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        filter_ = java.util.Collections.unmodifiableList(filter_);
        bitField0_ = (bitField0_ & ~0x00000001);
      }
      result.filter_ = filter_;
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
      if (other instanceof alluxio.grpc.GetMasterInfoPOptions) {
        return mergeFrom((alluxio.grpc.GetMasterInfoPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.GetMasterInfoPOptions other) {
      if (other == alluxio.grpc.GetMasterInfoPOptions.getDefaultInstance()) return this;
      if (!other.filter_.isEmpty()) {
        if (filter_.isEmpty()) {
          filter_ = other.filter_;
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          ensureFilterIsMutable();
          filter_.addAll(other.filter_);
        }
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
      alluxio.grpc.GetMasterInfoPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.GetMasterInfoPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<java.lang.Integer> filter_ =
      java.util.Collections.emptyList();
    private void ensureFilterIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        filter_ = new java.util.ArrayList<java.lang.Integer>(filter_);
        bitField0_ |= 0x00000001;
      }
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public java.util.List<alluxio.grpc.MasterInfoField> getFilterList() {
      return new com.google.protobuf.Internal.ListAdapter<
          java.lang.Integer, alluxio.grpc.MasterInfoField>(filter_, filter_converter_);
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public int getFilterCount() {
      return filter_.size();
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public alluxio.grpc.MasterInfoField getFilter(int index) {
      return filter_converter_.convert(filter_.get(index));
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public Builder setFilter(
        int index, alluxio.grpc.MasterInfoField value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureFilterIsMutable();
      filter_.set(index, value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public Builder addFilter(alluxio.grpc.MasterInfoField value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureFilterIsMutable();
      filter_.add(value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public Builder addAllFilter(
        java.lang.Iterable<? extends alluxio.grpc.MasterInfoField> values) {
      ensureFilterIsMutable();
      for (alluxio.grpc.MasterInfoField value : values) {
        filter_.add(value.getNumber());
      }
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.MasterInfoField filter = 1;</code>
     */
    public Builder clearFilter() {
      filter_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000001);
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.GetMasterInfoPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.GetMasterInfoPOptions)
  private static final alluxio.grpc.GetMasterInfoPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.GetMasterInfoPOptions();
  }

  public static alluxio.grpc.GetMasterInfoPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetMasterInfoPOptions>
      PARSER = new com.google.protobuf.AbstractParser<GetMasterInfoPOptions>() {
    public GetMasterInfoPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetMasterInfoPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetMasterInfoPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetMasterInfoPOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.GetMasterInfoPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

