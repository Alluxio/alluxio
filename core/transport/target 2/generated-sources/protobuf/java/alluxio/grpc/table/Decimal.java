// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf type {@code alluxio.grpc.table.Decimal}
 */
public final class Decimal extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.table.Decimal)
    DecimalOrBuilder {
private static final long serialVersionUID = 0L;
  // Use Decimal.newBuilder() to construct.
  private Decimal(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Decimal() {
    unscaled_ = com.google.protobuf.ByteString.EMPTY;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new Decimal();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private Decimal(
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
          case 8: {
            bitField0_ |= 0x00000001;
            scale_ = input.readInt32();
            break;
          }
          case 18: {
            bitField0_ |= 0x00000002;
            unscaled_ = input.readBytes();
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
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Decimal_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Decimal_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.table.Decimal.class, alluxio.grpc.table.Decimal.Builder.class);
  }

  private int bitField0_;
  public static final int SCALE_FIELD_NUMBER = 1;
  private int scale_;
  /**
   * <pre>
   * force using scale first in Decimal.compareTo
   * </pre>
   *
   * <code>required int32 scale = 1;</code>
   * @return Whether the scale field is set.
   */
  @java.lang.Override
  public boolean hasScale() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <pre>
   * force using scale first in Decimal.compareTo
   * </pre>
   *
   * <code>required int32 scale = 1;</code>
   * @return The scale.
   */
  @java.lang.Override
  public int getScale() {
    return scale_;
  }

  public static final int UNSCALED_FIELD_NUMBER = 2;
  private com.google.protobuf.ByteString unscaled_;
  /**
   * <code>required bytes unscaled = 2;</code>
   * @return Whether the unscaled field is set.
   */
  @java.lang.Override
  public boolean hasUnscaled() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>required bytes unscaled = 2;</code>
   * @return The unscaled.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString getUnscaled() {
    return unscaled_;
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    if (!hasScale()) {
      memoizedIsInitialized = 0;
      return false;
    }
    if (!hasUnscaled()) {
      memoizedIsInitialized = 0;
      return false;
    }
    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) != 0)) {
      output.writeInt32(1, scale_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      output.writeBytes(2, unscaled_);
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
        .computeInt32Size(1, scale_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(2, unscaled_);
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
    if (!(obj instanceof alluxio.grpc.table.Decimal)) {
      return super.equals(obj);
    }
    alluxio.grpc.table.Decimal other = (alluxio.grpc.table.Decimal) obj;

    if (hasScale() != other.hasScale()) return false;
    if (hasScale()) {
      if (getScale()
          != other.getScale()) return false;
    }
    if (hasUnscaled() != other.hasUnscaled()) return false;
    if (hasUnscaled()) {
      if (!getUnscaled()
          .equals(other.getUnscaled())) return false;
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
    if (hasScale()) {
      hash = (37 * hash) + SCALE_FIELD_NUMBER;
      hash = (53 * hash) + getScale();
    }
    if (hasUnscaled()) {
      hash = (37 * hash) + UNSCALED_FIELD_NUMBER;
      hash = (53 * hash) + getUnscaled().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.table.Decimal parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Decimal parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Decimal parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Decimal parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Decimal parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Decimal parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.table.Decimal prototype) {
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
   * Protobuf type {@code alluxio.grpc.table.Decimal}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.table.Decimal)
      alluxio.grpc.table.DecimalOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Decimal_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Decimal_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.table.Decimal.class, alluxio.grpc.table.Decimal.Builder.class);
    }

    // Construct using alluxio.grpc.table.Decimal.newBuilder()
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
      scale_ = 0;
      bitField0_ = (bitField0_ & ~0x00000001);
      unscaled_ = com.google.protobuf.ByteString.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Decimal_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.table.Decimal getDefaultInstanceForType() {
      return alluxio.grpc.table.Decimal.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.table.Decimal build() {
      alluxio.grpc.table.Decimal result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.table.Decimal buildPartial() {
      alluxio.grpc.table.Decimal result = new alluxio.grpc.table.Decimal(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.scale_ = scale_;
        to_bitField0_ |= 0x00000001;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        to_bitField0_ |= 0x00000002;
      }
      result.unscaled_ = unscaled_;
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
      if (other instanceof alluxio.grpc.table.Decimal) {
        return mergeFrom((alluxio.grpc.table.Decimal)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.table.Decimal other) {
      if (other == alluxio.grpc.table.Decimal.getDefaultInstance()) return this;
      if (other.hasScale()) {
        setScale(other.getScale());
      }
      if (other.hasUnscaled()) {
        setUnscaled(other.getUnscaled());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      if (!hasScale()) {
        return false;
      }
      if (!hasUnscaled()) {
        return false;
      }
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.table.Decimal parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.table.Decimal) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int scale_ ;
    /**
     * <pre>
     * force using scale first in Decimal.compareTo
     * </pre>
     *
     * <code>required int32 scale = 1;</code>
     * @return Whether the scale field is set.
     */
    @java.lang.Override
    public boolean hasScale() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <pre>
     * force using scale first in Decimal.compareTo
     * </pre>
     *
     * <code>required int32 scale = 1;</code>
     * @return The scale.
     */
    @java.lang.Override
    public int getScale() {
      return scale_;
    }
    /**
     * <pre>
     * force using scale first in Decimal.compareTo
     * </pre>
     *
     * <code>required int32 scale = 1;</code>
     * @param value The scale to set.
     * @return This builder for chaining.
     */
    public Builder setScale(int value) {
      bitField0_ |= 0x00000001;
      scale_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * force using scale first in Decimal.compareTo
     * </pre>
     *
     * <code>required int32 scale = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearScale() {
      bitField0_ = (bitField0_ & ~0x00000001);
      scale_ = 0;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString unscaled_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>required bytes unscaled = 2;</code>
     * @return Whether the unscaled field is set.
     */
    @java.lang.Override
    public boolean hasUnscaled() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>required bytes unscaled = 2;</code>
     * @return The unscaled.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getUnscaled() {
      return unscaled_;
    }
    /**
     * <code>required bytes unscaled = 2;</code>
     * @param value The unscaled to set.
     * @return This builder for chaining.
     */
    public Builder setUnscaled(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      unscaled_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>required bytes unscaled = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearUnscaled() {
      bitField0_ = (bitField0_ & ~0x00000002);
      unscaled_ = getDefaultInstance().getUnscaled();
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.table.Decimal)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.table.Decimal)
  private static final alluxio.grpc.table.Decimal DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.table.Decimal();
  }

  public static alluxio.grpc.table.Decimal getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<Decimal>
      PARSER = new com.google.protobuf.AbstractParser<Decimal>() {
    @java.lang.Override
    public Decimal parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new Decimal(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Decimal> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Decimal> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.table.Decimal getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

