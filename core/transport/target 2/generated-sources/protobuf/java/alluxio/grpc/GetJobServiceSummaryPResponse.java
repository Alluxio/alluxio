// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.job.GetJobServiceSummaryPResponse}
 */
public final class GetJobServiceSummaryPResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.job.GetJobServiceSummaryPResponse)
    GetJobServiceSummaryPResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use GetJobServiceSummaryPResponse.newBuilder() to construct.
  private GetJobServiceSummaryPResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private GetJobServiceSummaryPResponse() {
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new GetJobServiceSummaryPResponse();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private GetJobServiceSummaryPResponse(
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
            alluxio.grpc.JobServiceSummary.Builder subBuilder = null;
            if (((bitField0_ & 0x00000001) != 0)) {
              subBuilder = summary_.toBuilder();
            }
            summary_ = input.readMessage(alluxio.grpc.JobServiceSummary.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(summary_);
              summary_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000001;
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
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_GetJobServiceSummaryPResponse_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_GetJobServiceSummaryPResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.GetJobServiceSummaryPResponse.class, alluxio.grpc.GetJobServiceSummaryPResponse.Builder.class);
  }

  private int bitField0_;
  public static final int SUMMARY_FIELD_NUMBER = 1;
  private alluxio.grpc.JobServiceSummary summary_;
  /**
   * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
   * @return Whether the summary field is set.
   */
  @java.lang.Override
  public boolean hasSummary() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
   * @return The summary.
   */
  @java.lang.Override
  public alluxio.grpc.JobServiceSummary getSummary() {
    return summary_ == null ? alluxio.grpc.JobServiceSummary.getDefaultInstance() : summary_;
  }
  /**
   * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.JobServiceSummaryOrBuilder getSummaryOrBuilder() {
    return summary_ == null ? alluxio.grpc.JobServiceSummary.getDefaultInstance() : summary_;
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
      output.writeMessage(1, getSummary());
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
        .computeMessageSize(1, getSummary());
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
    if (!(obj instanceof alluxio.grpc.GetJobServiceSummaryPResponse)) {
      return super.equals(obj);
    }
    alluxio.grpc.GetJobServiceSummaryPResponse other = (alluxio.grpc.GetJobServiceSummaryPResponse) obj;

    if (hasSummary() != other.hasSummary()) return false;
    if (hasSummary()) {
      if (!getSummary()
          .equals(other.getSummary())) return false;
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
    if (hasSummary()) {
      hash = (37 * hash) + SUMMARY_FIELD_NUMBER;
      hash = (53 * hash) + getSummary().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.GetJobServiceSummaryPResponse parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.GetJobServiceSummaryPResponse prototype) {
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
   * Protobuf type {@code alluxio.grpc.job.GetJobServiceSummaryPResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.job.GetJobServiceSummaryPResponse)
      alluxio.grpc.GetJobServiceSummaryPResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_GetJobServiceSummaryPResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_GetJobServiceSummaryPResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.GetJobServiceSummaryPResponse.class, alluxio.grpc.GetJobServiceSummaryPResponse.Builder.class);
    }

    // Construct using alluxio.grpc.GetJobServiceSummaryPResponse.newBuilder()
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
        getSummaryFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      if (summaryBuilder_ == null) {
        summary_ = null;
      } else {
        summaryBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_GetJobServiceSummaryPResponse_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.GetJobServiceSummaryPResponse getDefaultInstanceForType() {
      return alluxio.grpc.GetJobServiceSummaryPResponse.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.GetJobServiceSummaryPResponse build() {
      alluxio.grpc.GetJobServiceSummaryPResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.GetJobServiceSummaryPResponse buildPartial() {
      alluxio.grpc.GetJobServiceSummaryPResponse result = new alluxio.grpc.GetJobServiceSummaryPResponse(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        if (summaryBuilder_ == null) {
          result.summary_ = summary_;
        } else {
          result.summary_ = summaryBuilder_.build();
        }
        to_bitField0_ |= 0x00000001;
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
      if (other instanceof alluxio.grpc.GetJobServiceSummaryPResponse) {
        return mergeFrom((alluxio.grpc.GetJobServiceSummaryPResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.GetJobServiceSummaryPResponse other) {
      if (other == alluxio.grpc.GetJobServiceSummaryPResponse.getDefaultInstance()) return this;
      if (other.hasSummary()) {
        mergeSummary(other.getSummary());
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
      alluxio.grpc.GetJobServiceSummaryPResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.GetJobServiceSummaryPResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private alluxio.grpc.JobServiceSummary summary_;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.JobServiceSummary, alluxio.grpc.JobServiceSummary.Builder, alluxio.grpc.JobServiceSummaryOrBuilder> summaryBuilder_;
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     * @return Whether the summary field is set.
     */
    public boolean hasSummary() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     * @return The summary.
     */
    public alluxio.grpc.JobServiceSummary getSummary() {
      if (summaryBuilder_ == null) {
        return summary_ == null ? alluxio.grpc.JobServiceSummary.getDefaultInstance() : summary_;
      } else {
        return summaryBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public Builder setSummary(alluxio.grpc.JobServiceSummary value) {
      if (summaryBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        summary_ = value;
        onChanged();
      } else {
        summaryBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public Builder setSummary(
        alluxio.grpc.JobServiceSummary.Builder builderForValue) {
      if (summaryBuilder_ == null) {
        summary_ = builderForValue.build();
        onChanged();
      } else {
        summaryBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public Builder mergeSummary(alluxio.grpc.JobServiceSummary value) {
      if (summaryBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0) &&
            summary_ != null &&
            summary_ != alluxio.grpc.JobServiceSummary.getDefaultInstance()) {
          summary_ =
            alluxio.grpc.JobServiceSummary.newBuilder(summary_).mergeFrom(value).buildPartial();
        } else {
          summary_ = value;
        }
        onChanged();
      } else {
        summaryBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000001;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public Builder clearSummary() {
      if (summaryBuilder_ == null) {
        summary_ = null;
        onChanged();
      } else {
        summaryBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public alluxio.grpc.JobServiceSummary.Builder getSummaryBuilder() {
      bitField0_ |= 0x00000001;
      onChanged();
      return getSummaryFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    public alluxio.grpc.JobServiceSummaryOrBuilder getSummaryOrBuilder() {
      if (summaryBuilder_ != null) {
        return summaryBuilder_.getMessageOrBuilder();
      } else {
        return summary_ == null ?
            alluxio.grpc.JobServiceSummary.getDefaultInstance() : summary_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.job.JobServiceSummary summary = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.JobServiceSummary, alluxio.grpc.JobServiceSummary.Builder, alluxio.grpc.JobServiceSummaryOrBuilder> 
        getSummaryFieldBuilder() {
      if (summaryBuilder_ == null) {
        summaryBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.JobServiceSummary, alluxio.grpc.JobServiceSummary.Builder, alluxio.grpc.JobServiceSummaryOrBuilder>(
                getSummary(),
                getParentForChildren(),
                isClean());
        summary_ = null;
      }
      return summaryBuilder_;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.job.GetJobServiceSummaryPResponse)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.job.GetJobServiceSummaryPResponse)
  private static final alluxio.grpc.GetJobServiceSummaryPResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.GetJobServiceSummaryPResponse();
  }

  public static alluxio.grpc.GetJobServiceSummaryPResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<GetJobServiceSummaryPResponse>
      PARSER = new com.google.protobuf.AbstractParser<GetJobServiceSummaryPResponse>() {
    @java.lang.Override
    public GetJobServiceSummaryPResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new GetJobServiceSummaryPResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<GetJobServiceSummaryPResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<GetJobServiceSummaryPResponse> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.GetJobServiceSummaryPResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

