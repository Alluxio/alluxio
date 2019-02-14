// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.GetFileInfoPResponse}
 */
public  final class GetFileInfoPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.GetFileInfoPResponse)
    GetFileInfoPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetFileInfoPResponse.newBuilder() to construct.
  private GetFileInfoPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetFileInfoPResponse() {
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetFileInfoPResponse(
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
            alluxio.grpc.FileInfo.Builder subBuilder = null;
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
              subBuilder = fileInfo_.toBuilder();
            }
            fileInfo_ = input.readMessage(alluxio.grpc.FileInfo.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(fileInfo_);
              fileInfo_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000001;
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_GetFileInfoPResponse_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_GetFileInfoPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.GetFileInfoPResponse.class, alluxio.grpc.GetFileInfoPResponse.Builder.class);
  }

  private int bitField0_;
  public static final int FILEINFO_FIELD_NUMBER = 1;
  private alluxio.grpc.FileInfo fileInfo_;
  /**
   * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
   */
  public boolean hasFileInfo() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
   */
  public alluxio.grpc.FileInfo getFileInfo() {
    return fileInfo_ == null ? alluxio.grpc.FileInfo.getDefaultInstance() : fileInfo_;
  }
  /**
   * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
   */
  public alluxio.grpc.FileInfoOrBuilder getFileInfoOrBuilder() {
    return fileInfo_ == null ? alluxio.grpc.FileInfo.getDefaultInstance() : fileInfo_;
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
      output.writeMessage(1, getFileInfo());
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getFileInfo());
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
    if (!(obj instanceof alluxio.grpc.GetFileInfoPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.GetFileInfoPResponse other = (alluxio.grpc.GetFileInfoPResponse) obj;

    boolean result = true;
    result = result && (hasFileInfo() == other.hasFileInfo());
    if (hasFileInfo()) {
      result = result && getFileInfo()
          .equals(other.getFileInfo());
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
    if (hasFileInfo()) {
      hash = (37 * hash) + FILEINFO_FIELD_NUMBER;
      hash = (53 * hash) + getFileInfo().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetFileInfoPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.GetFileInfoPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.GetFileInfoPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.GetFileInfoPResponse)
      alluxio.grpc.GetFileInfoPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_GetFileInfoPResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_GetFileInfoPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.GetFileInfoPResponse.class, alluxio.grpc.GetFileInfoPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.GetFileInfoPResponse.newBuilder()
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
        getFileInfoFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (fileInfoBuilder_ == null) {
        fileInfo_ = null;
      } else {
        fileInfoBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_GetFileInfoPResponse_descriptor;
    }

    public alluxio.grpc.GetFileInfoPResponse getDefaultInstanceForType() {
      return alluxio.grpc.GetFileInfoPResponse.getDefaultInstance();
    }

    public alluxio.grpc.GetFileInfoPResponse build() {
      alluxio.grpc.GetFileInfoPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.GetFileInfoPResponse buildPartial() {
      alluxio.grpc.GetFileInfoPResponse result = new alluxio.grpc.GetFileInfoPResponse(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      if (fileInfoBuilder_ == null) {
        result.fileInfo_ = fileInfo_;
      } else {
        result.fileInfo_ = fileInfoBuilder_.build();
      }
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
      if (other instanceof alluxio.grpc.GetFileInfoPResponse) {
        return mergeFrom((alluxio.grpc.GetFileInfoPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.GetFileInfoPResponse other) {
      if (other == alluxio.grpc.GetFileInfoPResponse.getDefaultInstance()) return this;
      if (other.hasFileInfo()) {
        mergeFileInfo(other.getFileInfo());
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
      alluxio.grpc.GetFileInfoPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.GetFileInfoPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private alluxio.grpc.FileInfo fileInfo_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder> fileInfoBuilder_;
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public boolean hasFileInfo() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public alluxio.grpc.FileInfo getFileInfo() {
      if (fileInfoBuilder_ == null) {
        return fileInfo_ == null ? alluxio.grpc.FileInfo.getDefaultInstance() : fileInfo_;
      } else {
        return fileInfoBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public Builder setFileInfo(alluxio.grpc.FileInfo value) {
      if (fileInfoBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        fileInfo_ = value;
        onChanged();
      } else {
        fileInfoBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public Builder setFileInfo(
        alluxio.grpc.FileInfo.Builder builderForValue) {
      if (fileInfoBuilder_ == null) {
        fileInfo_ = builderForValue.build();
        onChanged();
      } else {
        fileInfoBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public Builder mergeFileInfo(alluxio.grpc.FileInfo value) {
      if (fileInfoBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001) &&
            fileInfo_ != null &&
            fileInfo_ != alluxio.grpc.FileInfo.getDefaultInstance()) {
          fileInfo_ =
            alluxio.grpc.FileInfo.newBuilder(fileInfo_).mergeFrom(value).buildPartial();
        } else {
          fileInfo_ = value;
        }
        onChanged();
      } else {
        fileInfoBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public Builder clearFileInfo() {
      if (fileInfoBuilder_ == null) {
        fileInfo_ = null;
        onChanged();
      } else {
        fileInfoBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public alluxio.grpc.FileInfo.Builder getFileInfoBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getFileInfoFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    public alluxio.grpc.FileInfoOrBuilder getFileInfoOrBuilder() {
      if (fileInfoBuilder_ != null) {
        return fileInfoBuilder_.getMessageOrBuilder();
      } else {
        return fileInfo_ == null ?
            alluxio.grpc.FileInfo.getDefaultInstance() : fileInfo_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileInfo fileInfo = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder> 
        getFileInfoFieldBuilder() {
      if (fileInfoBuilder_ == null) {
        fileInfoBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.FileInfo, alluxio.grpc.FileInfo.Builder, alluxio.grpc.FileInfoOrBuilder>(
                getFileInfo(),
                getParentForChildren(),
                isClean());
        fileInfo_ = null;
      }
      return fileInfoBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.GetFileInfoPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.GetFileInfoPResponse)
  private static final alluxio.grpc.GetFileInfoPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.GetFileInfoPResponse();
  }

  public static alluxio.grpc.GetFileInfoPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetFileInfoPResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetFileInfoPResponse>() {
    public GetFileInfoPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetFileInfoPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetFileInfoPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetFileInfoPResponse> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.GetFileInfoPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

