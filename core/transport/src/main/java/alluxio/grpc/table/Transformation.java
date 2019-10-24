// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf type {@code alluxio.grpc.table.Transformation}
 */
public  final class Transformation extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.table.Transformation)
    TransformationOrBuilder {
private static final long serialVersionUID = 0L;
  // Use Transformation.newBuilder() to construct.
  private Transformation(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Transformation() {
    definition_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private Transformation(
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
            alluxio.grpc.table.Layout.Builder subBuilder = null;
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
              subBuilder = layout_.toBuilder();
            }
            layout_ = input.readMessage(alluxio.grpc.table.Layout.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(layout_);
              layout_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000001;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            definition_ = bs;
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
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Transformation_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Transformation_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.table.Transformation.class, alluxio.grpc.table.Transformation.Builder.class);
  }

  private int bitField0_;
  public static final int LAYOUT_FIELD_NUMBER = 1;
  private alluxio.grpc.table.Layout layout_;
  /**
   * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
   */
  public boolean hasLayout() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
   */
  public alluxio.grpc.table.Layout getLayout() {
    return layout_ == null ? alluxio.grpc.table.Layout.getDefaultInstance() : layout_;
  }
  /**
   * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
   */
  public alluxio.grpc.table.LayoutOrBuilder getLayoutOrBuilder() {
    return layout_ == null ? alluxio.grpc.table.Layout.getDefaultInstance() : layout_;
  }

  public static final int DEFINITION_FIELD_NUMBER = 2;
  private volatile java.lang.Object definition_;
  /**
   * <code>optional string definition = 2;</code>
   */
  public boolean hasDefinition() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string definition = 2;</code>
   */
  public java.lang.String getDefinition() {
    java.lang.Object ref = definition_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        definition_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string definition = 2;</code>
   */
  public com.google.protobuf.ByteString
      getDefinitionBytes() {
    java.lang.Object ref = definition_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      definition_ = b;
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

    if (hasLayout()) {
      if (!getLayout().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
    }
    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeMessage(1, getLayout());
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, definition_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getLayout());
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, definition_);
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
    if (!(obj instanceof alluxio.grpc.table.Transformation)) {
      return super.equals(obj);
    }
    alluxio.grpc.table.Transformation other = (alluxio.grpc.table.Transformation) obj;

    boolean result = true;
    result = result && (hasLayout() == other.hasLayout());
    if (hasLayout()) {
      result = result && getLayout()
          .equals(other.getLayout());
    }
    result = result && (hasDefinition() == other.hasDefinition());
    if (hasDefinition()) {
      result = result && getDefinition()
          .equals(other.getDefinition());
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
    if (hasLayout()) {
      hash = (37 * hash) + LAYOUT_FIELD_NUMBER;
      hash = (53 * hash) + getLayout().hashCode();
    }
    if (hasDefinition()) {
      hash = (37 * hash) + DEFINITION_FIELD_NUMBER;
      hash = (53 * hash) + getDefinition().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.table.Transformation parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Transformation parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Transformation parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Transformation parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Transformation parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Transformation parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.table.Transformation prototype) {
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
   * Protobuf type {@code alluxio.grpc.table.Transformation}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.table.Transformation)
      alluxio.grpc.table.TransformationOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Transformation_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Transformation_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.table.Transformation.class, alluxio.grpc.table.Transformation.Builder.class);
    }

    // Construct using alluxio.grpc.table.Transformation.newBuilder()
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
        getLayoutFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (layoutBuilder_ == null) {
        layout_ = null;
      } else {
        layoutBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      definition_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Transformation_descriptor;
    }

    public alluxio.grpc.table.Transformation getDefaultInstanceForType() {
      return alluxio.grpc.table.Transformation.getDefaultInstance();
    }

    public alluxio.grpc.table.Transformation build() {
      alluxio.grpc.table.Transformation result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.table.Transformation buildPartial() {
      alluxio.grpc.table.Transformation result = new alluxio.grpc.table.Transformation(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      if (layoutBuilder_ == null) {
        result.layout_ = layout_;
      } else {
        result.layout_ = layoutBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.definition_ = definition_;
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
      if (other instanceof alluxio.grpc.table.Transformation) {
        return mergeFrom((alluxio.grpc.table.Transformation)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.table.Transformation other) {
      if (other == alluxio.grpc.table.Transformation.getDefaultInstance()) return this;
      if (other.hasLayout()) {
        mergeLayout(other.getLayout());
      }
      if (other.hasDefinition()) {
        bitField0_ |= 0x00000002;
        definition_ = other.definition_;
        onChanged();
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      if (hasLayout()) {
        if (!getLayout().isInitialized()) {
          return false;
        }
      }
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.table.Transformation parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.table.Transformation) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private alluxio.grpc.table.Layout layout_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.table.Layout, alluxio.grpc.table.Layout.Builder, alluxio.grpc.table.LayoutOrBuilder> layoutBuilder_;
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public boolean hasLayout() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public alluxio.grpc.table.Layout getLayout() {
      if (layoutBuilder_ == null) {
        return layout_ == null ? alluxio.grpc.table.Layout.getDefaultInstance() : layout_;
      } else {
        return layoutBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public Builder setLayout(alluxio.grpc.table.Layout value) {
      if (layoutBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        layout_ = value;
        onChanged();
      } else {
        layoutBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public Builder setLayout(
        alluxio.grpc.table.Layout.Builder builderForValue) {
      if (layoutBuilder_ == null) {
        layout_ = builderForValue.build();
        onChanged();
      } else {
        layoutBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public Builder mergeLayout(alluxio.grpc.table.Layout value) {
      if (layoutBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001) &&
            layout_ != null &&
            layout_ != alluxio.grpc.table.Layout.getDefaultInstance()) {
          layout_ =
            alluxio.grpc.table.Layout.newBuilder(layout_).mergeFrom(value).buildPartial();
        } else {
          layout_ = value;
        }
        onChanged();
      } else {
        layoutBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public Builder clearLayout() {
      if (layoutBuilder_ == null) {
        layout_ = null;
        onChanged();
      } else {
        layoutBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public alluxio.grpc.table.Layout.Builder getLayoutBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getLayoutFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    public alluxio.grpc.table.LayoutOrBuilder getLayoutOrBuilder() {
      if (layoutBuilder_ != null) {
        return layoutBuilder_.getMessageOrBuilder();
      } else {
        return layout_ == null ?
            alluxio.grpc.table.Layout.getDefaultInstance() : layout_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.table.Layout layout = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.table.Layout, alluxio.grpc.table.Layout.Builder, alluxio.grpc.table.LayoutOrBuilder> 
        getLayoutFieldBuilder() {
      if (layoutBuilder_ == null) {
        layoutBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.table.Layout, alluxio.grpc.table.Layout.Builder, alluxio.grpc.table.LayoutOrBuilder>(
                getLayout(),
                getParentForChildren(),
                isClean());
        layout_ = null;
      }
      return layoutBuilder_;
    }

    private java.lang.Object definition_ = "";
    /**
     * <code>optional string definition = 2;</code>
     */
    public boolean hasDefinition() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string definition = 2;</code>
     */
    public java.lang.String getDefinition() {
      java.lang.Object ref = definition_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          definition_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string definition = 2;</code>
     */
    public com.google.protobuf.ByteString
        getDefinitionBytes() {
      java.lang.Object ref = definition_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        definition_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string definition = 2;</code>
     */
    public Builder setDefinition(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      definition_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string definition = 2;</code>
     */
    public Builder clearDefinition() {
      bitField0_ = (bitField0_ & ~0x00000002);
      definition_ = getDefaultInstance().getDefinition();
      onChanged();
      return this;
    }
    /**
     * <code>optional string definition = 2;</code>
     */
    public Builder setDefinitionBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      definition_ = value;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.table.Transformation)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.table.Transformation)
  private static final alluxio.grpc.table.Transformation DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.table.Transformation();
  }

  public static alluxio.grpc.table.Transformation getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<Transformation>
      PARSER = new com.google.protobuf.AbstractParser<Transformation>() {
    public Transformation parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new Transformation(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Transformation> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Transformation> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.table.Transformation getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

