// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.block.GetWorkerReportPOptions}
 */
public final class GetWorkerReportPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.block.GetWorkerReportPOptions)
    GetWorkerReportPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetWorkerReportPOptions.newBuilder() to construct.
  private GetWorkerReportPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetWorkerReportPOptions() {
    addresses_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    fieldRanges_ = java.util.Collections.emptyList();
    workerRange_ = 1;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new GetWorkerReportPOptions();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetWorkerReportPOptions(
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
            if (!((mutable_bitField0_ & 0x00000001) != 0)) {
              addresses_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000001;
            }
            addresses_.add(bs);
            break;
          }
          case 16: {
            int rawValue = input.readEnum();
            @SuppressWarnings("deprecation")
            alluxio.grpc.WorkerInfoField value = alluxio.grpc.WorkerInfoField.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(2, rawValue);
            } else {
              if (!((mutable_bitField0_ & 0x00000002) != 0)) {
                fieldRanges_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000002;
              }
              fieldRanges_.add(rawValue);
            }
            break;
          }
          case 18: {
            int length = input.readRawVarint32();
            int oldLimit = input.pushLimit(length);
            while(input.getBytesUntilLimit() > 0) {
              int rawValue = input.readEnum();
              @SuppressWarnings("deprecation")
              alluxio.grpc.WorkerInfoField value = alluxio.grpc.WorkerInfoField.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                if (!((mutable_bitField0_ & 0x00000002) != 0)) {
                  fieldRanges_ = new java.util.ArrayList<java.lang.Integer>();
                  mutable_bitField0_ |= 0x00000002;
                }
                fieldRanges_.add(rawValue);
              }
            }
            input.popLimit(oldLimit);
            break;
          }
          case 24: {
            int rawValue = input.readEnum();
              @SuppressWarnings("deprecation")
            alluxio.grpc.WorkerRange value = alluxio.grpc.WorkerRange.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(3, rawValue);
            } else {
              bitField0_ |= 0x00000001;
              workerRange_ = rawValue;
            }
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
      if (((mutable_bitField0_ & 0x00000001) != 0)) {
        addresses_ = addresses_.getUnmodifiableView();
      }
      if (((mutable_bitField0_ & 0x00000002) != 0)) {
        fieldRanges_ = java.util.Collections.unmodifiableList(fieldRanges_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_block_GetWorkerReportPOptions_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_block_GetWorkerReportPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.GetWorkerReportPOptions.class, alluxio.grpc.GetWorkerReportPOptions.Builder.class);
  }

  private int bitField0_;
  public static final int ADDRESSES_FIELD_NUMBER = 1;
  private com.google.protobuf.LazyStringList addresses_;
  /**
   * <pre>
   ** addresses are only valid when workerRange is SPECIFIED 
   * </pre>
   *
   * <code>repeated string addresses = 1;</code>
   * @return A list containing the addresses.
   */
  public com.google.protobuf.ProtocolStringList
      getAddressesList() {
    return addresses_;
  }
  /**
   * <pre>
   ** addresses are only valid when workerRange is SPECIFIED 
   * </pre>
   *
   * <code>repeated string addresses = 1;</code>
   * @return The count of addresses.
   */
  public int getAddressesCount() {
    return addresses_.size();
  }
  /**
   * <pre>
   ** addresses are only valid when workerRange is SPECIFIED 
   * </pre>
   *
   * <code>repeated string addresses = 1;</code>
   * @param index The index of the element to return.
   * @return The addresses at the given index.
   */
  public java.lang.String getAddresses(int index) {
    return addresses_.get(index);
  }
  /**
   * <pre>
   ** addresses are only valid when workerRange is SPECIFIED 
   * </pre>
   *
   * <code>repeated string addresses = 1;</code>
   * @param index The index of the value to return.
   * @return The bytes of the addresses at the given index.
   */
  public com.google.protobuf.ByteString
      getAddressesBytes(int index) {
    return addresses_.getByteString(index);
  }

  public static final int FIELDRANGES_FIELD_NUMBER = 2;
  private java.util.List<java.lang.Integer> fieldRanges_;
  private static final com.google.protobuf.Internal.ListAdapter.Converter<
      java.lang.Integer, alluxio.grpc.WorkerInfoField> fieldRanges_converter_ =
          new com.google.protobuf.Internal.ListAdapter.Converter<
              java.lang.Integer, alluxio.grpc.WorkerInfoField>() {
            public alluxio.grpc.WorkerInfoField convert(java.lang.Integer from) {
              @SuppressWarnings("deprecation")
              alluxio.grpc.WorkerInfoField result = alluxio.grpc.WorkerInfoField.valueOf(from);
              return result == null ? alluxio.grpc.WorkerInfoField.ADDRESS : result;
            }
          };
  /**
   * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
   * @return A list containing the fieldRanges.
   */
  @java.lang.Override
  public java.util.List<alluxio.grpc.WorkerInfoField> getFieldRangesList() {
    return new com.google.protobuf.Internal.ListAdapter<
        java.lang.Integer, alluxio.grpc.WorkerInfoField>(fieldRanges_, fieldRanges_converter_);
  }
  /**
   * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
   * @return The count of fieldRanges.
   */
  @java.lang.Override
  public int getFieldRangesCount() {
    return fieldRanges_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
   * @param index The index of the element to return.
   * @return The fieldRanges at the given index.
   */
  @java.lang.Override
  public alluxio.grpc.WorkerInfoField getFieldRanges(int index) {
    return fieldRanges_converter_.convert(fieldRanges_.get(index));
  }

  public static final int WORKERRANGE_FIELD_NUMBER = 3;
  private int workerRange_;
  /**
   * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
   * @return Whether the workerRange field is set.
   */
  @java.lang.Override public boolean hasWorkerRange() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
   * @return The workerRange.
   */
  @java.lang.Override public alluxio.grpc.WorkerRange getWorkerRange() {
    @SuppressWarnings("deprecation")
    alluxio.grpc.WorkerRange result = alluxio.grpc.WorkerRange.valueOf(workerRange_);
    return result == null ? alluxio.grpc.WorkerRange.ALL : result;
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
    for (int i = 0; i < addresses_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, addresses_.getRaw(i));
    }
    for (int i = 0; i < fieldRanges_.size(); i++) {
      output.writeEnum(2, fieldRanges_.get(i));
    }
    if (((bitField0_ & 0x00000001) != 0)) {
      output.writeEnum(3, workerRange_);
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    {
      int dataSize = 0;
      for (int i = 0; i < addresses_.size(); i++) {
        dataSize += computeStringSizeNoTag(addresses_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getAddressesList().size();
    }
    {
      int dataSize = 0;
      for (int i = 0; i < fieldRanges_.size(); i++) {
        dataSize += com.google.protobuf.CodedOutputStream
          .computeEnumSizeNoTag(fieldRanges_.get(i));
      }
      size += dataSize;
      size += 1 * fieldRanges_.size();
    }
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(3, workerRange_);
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
    if (!(obj instanceof alluxio.grpc.GetWorkerReportPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.GetWorkerReportPOptions other = (alluxio.grpc.GetWorkerReportPOptions) obj;

    if (!getAddressesList()
        .equals(other.getAddressesList())) return false;
    if (!fieldRanges_.equals(other.fieldRanges_)) return false;
    if (hasWorkerRange() != other.hasWorkerRange()) return false;
    if (hasWorkerRange()) {
      if (workerRange_ != other.workerRange_) return false;
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
    if (getAddressesCount() > 0) {
      hash = (37 * hash) + ADDRESSES_FIELD_NUMBER;
      hash = (53 * hash) + getAddressesList().hashCode();
    }
    if (getFieldRangesCount() > 0) {
      hash = (37 * hash) + FIELDRANGES_FIELD_NUMBER;
      hash = (53 * hash) + fieldRanges_.hashCode();
    }
    if (hasWorkerRange()) {
      hash = (37 * hash) + WORKERRANGE_FIELD_NUMBER;
      hash = (53 * hash) + workerRange_;
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetWorkerReportPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.GetWorkerReportPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.block.GetWorkerReportPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.block.GetWorkerReportPOptions)
      alluxio.grpc.GetWorkerReportPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_block_GetWorkerReportPOptions_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_block_GetWorkerReportPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.GetWorkerReportPOptions.class, alluxio.grpc.GetWorkerReportPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.GetWorkerReportPOptions.newBuilder()
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
    @java.lang.Override
    public Builder clear() {
      super.clear();
      addresses_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      fieldRanges_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000002);
      workerRange_ = 1;
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_block_GetWorkerReportPOptions_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.GetWorkerReportPOptions getDefaultInstanceForType() {
      return alluxio.grpc.GetWorkerReportPOptions.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.GetWorkerReportPOptions build() {
      alluxio.grpc.GetWorkerReportPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.GetWorkerReportPOptions buildPartial() {
      alluxio.grpc.GetWorkerReportPOptions result = new alluxio.grpc.GetWorkerReportPOptions(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        addresses_ = addresses_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000001);
      }
      result.addresses_ = addresses_;
      if (((bitField0_ & 0x00000002) != 0)) {
        fieldRanges_ = java.util.Collections.unmodifiableList(fieldRanges_);
        bitField0_ = (bitField0_ & ~0x00000002);
      }
      result.fieldRanges_ = fieldRanges_;
      if (((from_bitField0_ & 0x00000004) != 0)) {
        to_bitField0_ |= 0x00000001;
      }
      result.workerRange_ = workerRange_;
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
      if (other instanceof alluxio.grpc.GetWorkerReportPOptions) {
        return mergeFrom((alluxio.grpc.GetWorkerReportPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.GetWorkerReportPOptions other) {
      if (other == alluxio.grpc.GetWorkerReportPOptions.getDefaultInstance()) return this;
      if (!other.addresses_.isEmpty()) {
        if (addresses_.isEmpty()) {
          addresses_ = other.addresses_;
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          ensureAddressesIsMutable();
          addresses_.addAll(other.addresses_);
        }
        onChanged();
      }
      if (!other.fieldRanges_.isEmpty()) {
        if (fieldRanges_.isEmpty()) {
          fieldRanges_ = other.fieldRanges_;
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          ensureFieldRangesIsMutable();
          fieldRanges_.addAll(other.fieldRanges_);
        }
        onChanged();
      }
      if (other.hasWorkerRange()) {
        setWorkerRange(other.getWorkerRange());
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
      alluxio.grpc.GetWorkerReportPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.GetWorkerReportPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.LazyStringList addresses_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureAddressesIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        addresses_ = new com.google.protobuf.LazyStringArrayList(addresses_);
        bitField0_ |= 0x00000001;
       }
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @return A list containing the addresses.
     */
    public com.google.protobuf.ProtocolStringList
        getAddressesList() {
      return addresses_.getUnmodifiableView();
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @return The count of addresses.
     */
    public int getAddressesCount() {
      return addresses_.size();
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param index The index of the element to return.
     * @return The addresses at the given index.
     */
    public java.lang.String getAddresses(int index) {
      return addresses_.get(index);
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param index The index of the value to return.
     * @return The bytes of the addresses at the given index.
     */
    public com.google.protobuf.ByteString
        getAddressesBytes(int index) {
      return addresses_.getByteString(index);
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param index The index to set the value at.
     * @param value The addresses to set.
     * @return This builder for chaining.
     */
    public Builder setAddresses(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureAddressesIsMutable();
      addresses_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param value The addresses to add.
     * @return This builder for chaining.
     */
    public Builder addAddresses(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureAddressesIsMutable();
      addresses_.add(value);
      onChanged();
      return this;
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param values The addresses to add.
     * @return This builder for chaining.
     */
    public Builder addAllAddresses(
        java.lang.Iterable<java.lang.String> values) {
      ensureAddressesIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, addresses_);
      onChanged();
      return this;
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearAddresses() {
      addresses_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <pre>
     ** addresses are only valid when workerRange is SPECIFIED 
     * </pre>
     *
     * <code>repeated string addresses = 1;</code>
     * @param value The bytes of the addresses to add.
     * @return This builder for chaining.
     */
    public Builder addAddressesBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureAddressesIsMutable();
      addresses_.add(value);
      onChanged();
      return this;
    }

    private java.util.List<java.lang.Integer> fieldRanges_ =
      java.util.Collections.emptyList();
    private void ensureFieldRangesIsMutable() {
      if (!((bitField0_ & 0x00000002) != 0)) {
        fieldRanges_ = new java.util.ArrayList<java.lang.Integer>(fieldRanges_);
        bitField0_ |= 0x00000002;
      }
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @return A list containing the fieldRanges.
     */
    public java.util.List<alluxio.grpc.WorkerInfoField> getFieldRangesList() {
      return new com.google.protobuf.Internal.ListAdapter<
          java.lang.Integer, alluxio.grpc.WorkerInfoField>(fieldRanges_, fieldRanges_converter_);
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @return The count of fieldRanges.
     */
    public int getFieldRangesCount() {
      return fieldRanges_.size();
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @param index The index of the element to return.
     * @return The fieldRanges at the given index.
     */
    public alluxio.grpc.WorkerInfoField getFieldRanges(int index) {
      return fieldRanges_converter_.convert(fieldRanges_.get(index));
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @param index The index to set the value at.
     * @param value The fieldRanges to set.
     * @return This builder for chaining.
     */
    public Builder setFieldRanges(
        int index, alluxio.grpc.WorkerInfoField value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureFieldRangesIsMutable();
      fieldRanges_.set(index, value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @param value The fieldRanges to add.
     * @return This builder for chaining.
     */
    public Builder addFieldRanges(alluxio.grpc.WorkerInfoField value) {
      if (value == null) {
        throw new NullPointerException();
      }
      ensureFieldRangesIsMutable();
      fieldRanges_.add(value.getNumber());
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @param values The fieldRanges to add.
     * @return This builder for chaining.
     */
    public Builder addAllFieldRanges(
        java.lang.Iterable<? extends alluxio.grpc.WorkerInfoField> values) {
      ensureFieldRangesIsMutable();
      for (alluxio.grpc.WorkerInfoField value : values) {
        fieldRanges_.add(value.getNumber());
      }
      onChanged();
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.block.WorkerInfoField fieldRanges = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearFieldRanges() {
      fieldRanges_ = java.util.Collections.emptyList();
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }

    private int workerRange_ = 1;
    /**
     * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
     * @return Whether the workerRange field is set.
     */
    @java.lang.Override public boolean hasWorkerRange() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
     * @return The workerRange.
     */
    @java.lang.Override
    public alluxio.grpc.WorkerRange getWorkerRange() {
      @SuppressWarnings("deprecation")
      alluxio.grpc.WorkerRange result = alluxio.grpc.WorkerRange.valueOf(workerRange_);
      return result == null ? alluxio.grpc.WorkerRange.ALL : result;
    }
    /**
     * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
     * @param value The workerRange to set.
     * @return This builder for chaining.
     */
    public Builder setWorkerRange(alluxio.grpc.WorkerRange value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000004;
      workerRange_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.block.WorkerRange workerRange = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearWorkerRange() {
      bitField0_ = (bitField0_ & ~0x00000004);
      workerRange_ = 1;
      onChanged();
      return this;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.block.GetWorkerReportPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.block.GetWorkerReportPOptions)
  private static final alluxio.grpc.GetWorkerReportPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.GetWorkerReportPOptions();
  }

  public static alluxio.grpc.GetWorkerReportPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetWorkerReportPOptions>
      PARSER = new com.google.protobuf.AbstractParser<GetWorkerReportPOptions>() {
    @java.lang.Override
    public GetWorkerReportPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetWorkerReportPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetWorkerReportPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetWorkerReportPOptions> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.GetWorkerReportPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

