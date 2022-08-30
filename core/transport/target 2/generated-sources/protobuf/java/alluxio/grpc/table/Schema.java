// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf type {@code alluxio.grpc.table.Schema}
 */
public final class Schema extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.table.Schema)
    SchemaOrBuilder {
private static final long serialVersionUID = 0L;
  // Use Schema.newBuilder() to construct.
  private Schema(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Schema() {
    cols_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new Schema();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private Schema(
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
            if (!((mutable_bitField0_ & 0x00000001) != 0)) {
              cols_ = new java.util.ArrayList<alluxio.grpc.table.FieldSchema>();
              mutable_bitField0_ |= 0x00000001;
            }
            cols_.add(
                input.readMessage(alluxio.grpc.table.FieldSchema.PARSER, extensionRegistry));
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
        cols_ = java.util.Collections.unmodifiableList(cols_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Schema_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Schema_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.table.Schema.class, alluxio.grpc.table.Schema.Builder.class);
  }

  public static final int COLS_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.table.FieldSchema> cols_;
  /**
   * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
   */
  @java.lang.Override
  public java.util.List<alluxio.grpc.table.FieldSchema> getColsList() {
    return cols_;
  }
  /**
   * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
   */
  @java.lang.Override
  public java.util.List<? extends alluxio.grpc.table.FieldSchemaOrBuilder> 
      getColsOrBuilderList() {
    return cols_;
  }
  /**
   * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
   */
  @java.lang.Override
  public int getColsCount() {
    return cols_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.table.FieldSchema getCols(int index) {
    return cols_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.table.FieldSchemaOrBuilder getColsOrBuilder(
      int index) {
    return cols_.get(index);
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
    for (int i = 0; i < cols_.size(); i++) {
      output.writeMessage(1, cols_.get(i));
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < cols_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, cols_.get(i));
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
    if (!(obj instanceof alluxio.grpc.table.Schema)) {
      return super.equals(obj);
    }
    alluxio.grpc.table.Schema other = (alluxio.grpc.table.Schema) obj;

    if (!getColsList()
        .equals(other.getColsList())) return false;
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
    if (getColsCount() > 0) {
      hash = (37 * hash) + COLS_FIELD_NUMBER;
      hash = (53 * hash) + getColsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.table.Schema parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Schema parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.Schema parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Schema parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Schema parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.Schema parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.Schema parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.table.Schema prototype) {
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
   * Protobuf type {@code alluxio.grpc.table.Schema}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.table.Schema)
      alluxio.grpc.table.SchemaOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Schema_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Schema_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.table.Schema.class, alluxio.grpc.table.Schema.Builder.class);
    }

    // Construct using alluxio.grpc.table.Schema.newBuilder()
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
        getColsFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      if (colsBuilder_ == null) {
        cols_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        colsBuilder_.clear();
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_Schema_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.table.Schema getDefaultInstanceForType() {
      return alluxio.grpc.table.Schema.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.table.Schema build() {
      alluxio.grpc.table.Schema result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.table.Schema buildPartial() {
      alluxio.grpc.table.Schema result = new alluxio.grpc.table.Schema(this);
      int from_bitField0_ = bitField0_;
      if (colsBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0)) {
          cols_ = java.util.Collections.unmodifiableList(cols_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.cols_ = cols_;
      } else {
        result.cols_ = colsBuilder_.build();
      }
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
      if (other instanceof alluxio.grpc.table.Schema) {
        return mergeFrom((alluxio.grpc.table.Schema)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.table.Schema other) {
      if (other == alluxio.grpc.table.Schema.getDefaultInstance()) return this;
      if (colsBuilder_ == null) {
        if (!other.cols_.isEmpty()) {
          if (cols_.isEmpty()) {
            cols_ = other.cols_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureColsIsMutable();
            cols_.addAll(other.cols_);
          }
          onChanged();
        }
      } else {
        if (!other.cols_.isEmpty()) {
          if (colsBuilder_.isEmpty()) {
            colsBuilder_.dispose();
            colsBuilder_ = null;
            cols_ = other.cols_;
            bitField0_ = (bitField0_ & ~0x00000001);
            colsBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getColsFieldBuilder() : null;
          } else {
            colsBuilder_.addAllMessages(other.cols_);
          }
        }
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
      alluxio.grpc.table.Schema parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.table.Schema) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.table.FieldSchema> cols_ =
      java.util.Collections.emptyList();
    private void ensureColsIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        cols_ = new java.util.ArrayList<alluxio.grpc.table.FieldSchema>(cols_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.table.FieldSchema, alluxio.grpc.table.FieldSchema.Builder, alluxio.grpc.table.FieldSchemaOrBuilder> colsBuilder_;

    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public java.util.List<alluxio.grpc.table.FieldSchema> getColsList() {
      if (colsBuilder_ == null) {
        return java.util.Collections.unmodifiableList(cols_);
      } else {
        return colsBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public int getColsCount() {
      if (colsBuilder_ == null) {
        return cols_.size();
      } else {
        return colsBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public alluxio.grpc.table.FieldSchema getCols(int index) {
      if (colsBuilder_ == null) {
        return cols_.get(index);
      } else {
        return colsBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder setCols(
        int index, alluxio.grpc.table.FieldSchema value) {
      if (colsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureColsIsMutable();
        cols_.set(index, value);
        onChanged();
      } else {
        colsBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder setCols(
        int index, alluxio.grpc.table.FieldSchema.Builder builderForValue) {
      if (colsBuilder_ == null) {
        ensureColsIsMutable();
        cols_.set(index, builderForValue.build());
        onChanged();
      } else {
        colsBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder addCols(alluxio.grpc.table.FieldSchema value) {
      if (colsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureColsIsMutable();
        cols_.add(value);
        onChanged();
      } else {
        colsBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder addCols(
        int index, alluxio.grpc.table.FieldSchema value) {
      if (colsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureColsIsMutable();
        cols_.add(index, value);
        onChanged();
      } else {
        colsBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder addCols(
        alluxio.grpc.table.FieldSchema.Builder builderForValue) {
      if (colsBuilder_ == null) {
        ensureColsIsMutable();
        cols_.add(builderForValue.build());
        onChanged();
      } else {
        colsBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder addCols(
        int index, alluxio.grpc.table.FieldSchema.Builder builderForValue) {
      if (colsBuilder_ == null) {
        ensureColsIsMutable();
        cols_.add(index, builderForValue.build());
        onChanged();
      } else {
        colsBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder addAllCols(
        java.lang.Iterable<? extends alluxio.grpc.table.FieldSchema> values) {
      if (colsBuilder_ == null) {
        ensureColsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, cols_);
        onChanged();
      } else {
        colsBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder clearCols() {
      if (colsBuilder_ == null) {
        cols_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        colsBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public Builder removeCols(int index) {
      if (colsBuilder_ == null) {
        ensureColsIsMutable();
        cols_.remove(index);
        onChanged();
      } else {
        colsBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public alluxio.grpc.table.FieldSchema.Builder getColsBuilder(
        int index) {
      return getColsFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public alluxio.grpc.table.FieldSchemaOrBuilder getColsOrBuilder(
        int index) {
      if (colsBuilder_ == null) {
        return cols_.get(index);  } else {
        return colsBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.table.FieldSchemaOrBuilder> 
         getColsOrBuilderList() {
      if (colsBuilder_ != null) {
        return colsBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(cols_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public alluxio.grpc.table.FieldSchema.Builder addColsBuilder() {
      return getColsFieldBuilder().addBuilder(
          alluxio.grpc.table.FieldSchema.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public alluxio.grpc.table.FieldSchema.Builder addColsBuilder(
        int index) {
      return getColsFieldBuilder().addBuilder(
          index, alluxio.grpc.table.FieldSchema.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.table.FieldSchema cols = 1;</code>
     */
    public java.util.List<alluxio.grpc.table.FieldSchema.Builder> 
         getColsBuilderList() {
      return getColsFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.table.FieldSchema, alluxio.grpc.table.FieldSchema.Builder, alluxio.grpc.table.FieldSchemaOrBuilder> 
        getColsFieldBuilder() {
      if (colsBuilder_ == null) {
        colsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.table.FieldSchema, alluxio.grpc.table.FieldSchema.Builder, alluxio.grpc.table.FieldSchemaOrBuilder>(
                cols_,
                ((bitField0_ & 0x00000001) != 0),
                getParentForChildren(),
                isClean());
        cols_ = null;
      }
      return colsBuilder_;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.table.Schema)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.table.Schema)
  private static final alluxio.grpc.table.Schema DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.table.Schema();
  }

  public static alluxio.grpc.table.Schema getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<Schema>
      PARSER = new com.google.protobuf.AbstractParser<Schema>() {
    @java.lang.Override
    public Schema parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new Schema(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Schema> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Schema> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.table.Schema getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

