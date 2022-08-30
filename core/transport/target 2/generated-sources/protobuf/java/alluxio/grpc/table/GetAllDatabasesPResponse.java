// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf type {@code alluxio.grpc.table.GetAllDatabasesPResponse}
 */
public final class GetAllDatabasesPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.table.GetAllDatabasesPResponse)
    GetAllDatabasesPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetAllDatabasesPResponse.newBuilder() to construct.
  private GetAllDatabasesPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetAllDatabasesPResponse() {
    database_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new GetAllDatabasesPResponse();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetAllDatabasesPResponse(
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
              database_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000001;
            }
            database_.add(bs);
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
        database_ = database_.getUnmodifiableView();
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_GetAllDatabasesPResponse_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_GetAllDatabasesPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.table.GetAllDatabasesPResponse.class, alluxio.grpc.table.GetAllDatabasesPResponse.Builder.class);
  }

  public static final int DATABASE_FIELD_NUMBER = 1;
  private com.google.protobuf.LazyStringList database_;
  /**
   * <code>repeated string database = 1;</code>
   * @return A list containing the database.
   */
  public com.google.protobuf.ProtocolStringList
      getDatabaseList() {
    return database_;
  }
  /**
   * <code>repeated string database = 1;</code>
   * @return The count of database.
   */
  public int getDatabaseCount() {
    return database_.size();
  }
  /**
   * <code>repeated string database = 1;</code>
   * @param index The index of the element to return.
   * @return The database at the given index.
   */
  public java.lang.String getDatabase(int index) {
    return database_.get(index);
  }
  /**
   * <code>repeated string database = 1;</code>
   * @param index The index of the value to return.
   * @return The bytes of the database at the given index.
   */
  public com.google.protobuf.ByteString
      getDatabaseBytes(int index) {
    return database_.getByteString(index);
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
    for (int i = 0; i < database_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, database_.getRaw(i));
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
      for (int i = 0; i < database_.size(); i++) {
        dataSize += computeStringSizeNoTag(database_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getDatabaseList().size();
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
    if (!(obj instanceof alluxio.grpc.table.GetAllDatabasesPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.table.GetAllDatabasesPResponse other = (alluxio.grpc.table.GetAllDatabasesPResponse) obj;

    if (!getDatabaseList()
        .equals(other.getDatabaseList())) return false;
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
    if (getDatabaseCount() > 0) {
      hash = (37 * hash) + DATABASE_FIELD_NUMBER;
      hash = (53 * hash) + getDatabaseList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.GetAllDatabasesPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.table.GetAllDatabasesPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.table.GetAllDatabasesPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.table.GetAllDatabasesPResponse)
      alluxio.grpc.table.GetAllDatabasesPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_GetAllDatabasesPResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_GetAllDatabasesPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.table.GetAllDatabasesPResponse.class, alluxio.grpc.table.GetAllDatabasesPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.table.GetAllDatabasesPResponse.newBuilder()
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
      database_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_GetAllDatabasesPResponse_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.table.GetAllDatabasesPResponse getDefaultInstanceForType() {
      return alluxio.grpc.table.GetAllDatabasesPResponse.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.table.GetAllDatabasesPResponse build() {
      alluxio.grpc.table.GetAllDatabasesPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.table.GetAllDatabasesPResponse buildPartial() {
      alluxio.grpc.table.GetAllDatabasesPResponse result = new alluxio.grpc.table.GetAllDatabasesPResponse(this);
      int from_bitField0_ = bitField0_;
      if (((bitField0_ & 0x00000001) != 0)) {
        database_ = database_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000001);
      }
      result.database_ = database_;
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
      if (other instanceof alluxio.grpc.table.GetAllDatabasesPResponse) {
        return mergeFrom((alluxio.grpc.table.GetAllDatabasesPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.table.GetAllDatabasesPResponse other) {
      if (other == alluxio.grpc.table.GetAllDatabasesPResponse.getDefaultInstance()) return this;
      if (!other.database_.isEmpty()) {
        if (database_.isEmpty()) {
          database_ = other.database_;
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          ensureDatabaseIsMutable();
          database_.addAll(other.database_);
        }
        onChanged();
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
      alluxio.grpc.table.GetAllDatabasesPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.table.GetAllDatabasesPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.LazyStringList database_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureDatabaseIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        database_ = new com.google.protobuf.LazyStringArrayList(database_);
        bitField0_ |= 0x00000001;
       }
    }
    /**
     * <code>repeated string database = 1;</code>
     * @return A list containing the database.
     */
    public com.google.protobuf.ProtocolStringList
        getDatabaseList() {
      return database_.getUnmodifiableView();
    }
    /**
     * <code>repeated string database = 1;</code>
     * @return The count of database.
     */
    public int getDatabaseCount() {
      return database_.size();
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param index The index of the element to return.
     * @return The database at the given index.
     */
    public java.lang.String getDatabase(int index) {
      return database_.get(index);
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param index The index of the value to return.
     * @return The bytes of the database at the given index.
     */
    public com.google.protobuf.ByteString
        getDatabaseBytes(int index) {
      return database_.getByteString(index);
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param index The index to set the value at.
     * @param value The database to set.
     * @return This builder for chaining.
     */
    public Builder setDatabase(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureDatabaseIsMutable();
      database_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param value The database to add.
     * @return This builder for chaining.
     */
    public Builder addDatabase(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureDatabaseIsMutable();
      database_.add(value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param values The database to add.
     * @return This builder for chaining.
     */
    public Builder addAllDatabase(
        java.lang.Iterable<java.lang.String> values) {
      ensureDatabaseIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, database_);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string database = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearDatabase() {
      database_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string database = 1;</code>
     * @param value The bytes of the database to add.
     * @return This builder for chaining.
     */
    public Builder addDatabaseBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureDatabaseIsMutable();
      database_.add(value);
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.table.GetAllDatabasesPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.table.GetAllDatabasesPResponse)
  private static final alluxio.grpc.table.GetAllDatabasesPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.table.GetAllDatabasesPResponse();
  }

  public static alluxio.grpc.table.GetAllDatabasesPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetAllDatabasesPResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetAllDatabasesPResponse>() {
    @java.lang.Override
    public GetAllDatabasesPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetAllDatabasesPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetAllDatabasesPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetAllDatabasesPResponse> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.table.GetAllDatabasesPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

