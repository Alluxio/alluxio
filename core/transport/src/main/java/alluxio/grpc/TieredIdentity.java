// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.TieredIdentity}
 */
public  final class TieredIdentity extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.TieredIdentity)
    TieredIdentityOrBuilder {
private static final long serialVersionUID = 0L;
  // Use TieredIdentity.newBuilder() to construct.
  private TieredIdentity(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private TieredIdentity() {
    tiers_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private TieredIdentity(
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
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
              tiers_ = new java.util.ArrayList<alluxio.grpc.LocalityTier>();
              mutable_bitField0_ |= 0x00000001;
            }
            tiers_.add(
                input.readMessage(alluxio.grpc.LocalityTier.PARSER, extensionRegistry));
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
        tiers_ = java.util.Collections.unmodifiableList(tiers_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_TieredIdentity_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_TieredIdentity_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.TieredIdentity.class, alluxio.grpc.TieredIdentity.Builder.class);
  }

  public static final int TIERS_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.LocalityTier> tiers_;
  /**
   * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
   */
  public java.util.List<alluxio.grpc.LocalityTier> getTiersList() {
    return tiers_;
  }
  /**
   * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
   */
  public java.util.List<? extends alluxio.grpc.LocalityTierOrBuilder> 
      getTiersOrBuilderList() {
    return tiers_;
  }
  /**
   * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
   */
  public int getTiersCount() {
    return tiers_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
   */
  public alluxio.grpc.LocalityTier getTiers(int index) {
    return tiers_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
   */
  public alluxio.grpc.LocalityTierOrBuilder getTiersOrBuilder(
      int index) {
    return tiers_.get(index);
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
    for (int i = 0; i < tiers_.size(); i++) {
      output.writeMessage(1, tiers_.get(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < tiers_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, tiers_.get(i));
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
    if (!(obj instanceof alluxio.grpc.TieredIdentity)) {
      return super.equals(obj);
    }
    alluxio.grpc.TieredIdentity other = (alluxio.grpc.TieredIdentity) obj;

    boolean result = true;
    result = result && getTiersList()
        .equals(other.getTiersList());
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
    if (getTiersCount() > 0) {
      hash = (37 * hash) + TIERS_FIELD_NUMBER;
      hash = (53 * hash) + getTiersList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.TieredIdentity parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.TieredIdentity parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.TieredIdentity parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.TieredIdentity parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.TieredIdentity prototype) {
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
   * Protobuf type {@code alluxio.grpc.TieredIdentity}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.TieredIdentity)
      alluxio.grpc.TieredIdentityOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_TieredIdentity_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_TieredIdentity_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.TieredIdentity.class, alluxio.grpc.TieredIdentity.Builder.class);
    }

    // Construct using alluxio.grpc.TieredIdentity.newBuilder()
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
        getTiersFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (tiersBuilder_ == null) {
        tiers_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        tiersBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.CommonProto.internal_static_alluxio_grpc_TieredIdentity_descriptor;
    }

    public alluxio.grpc.TieredIdentity getDefaultInstanceForType() {
      return alluxio.grpc.TieredIdentity.getDefaultInstance();
    }

    public alluxio.grpc.TieredIdentity build() {
      alluxio.grpc.TieredIdentity result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.TieredIdentity buildPartial() {
      alluxio.grpc.TieredIdentity result = new alluxio.grpc.TieredIdentity(this);
      int from_bitField0_ = bitField0_;
      if (tiersBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          tiers_ = java.util.Collections.unmodifiableList(tiers_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.tiers_ = tiers_;
      } else {
        result.tiers_ = tiersBuilder_.build();
      }
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
      if (other instanceof alluxio.grpc.TieredIdentity) {
        return mergeFrom((alluxio.grpc.TieredIdentity)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.TieredIdentity other) {
      if (other == alluxio.grpc.TieredIdentity.getDefaultInstance()) return this;
      if (tiersBuilder_ == null) {
        if (!other.tiers_.isEmpty()) {
          if (tiers_.isEmpty()) {
            tiers_ = other.tiers_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureTiersIsMutable();
            tiers_.addAll(other.tiers_);
          }
          onChanged();
        }
      } else {
        if (!other.tiers_.isEmpty()) {
          if (tiersBuilder_.isEmpty()) {
            tiersBuilder_.dispose();
            tiersBuilder_ = null;
            tiers_ = other.tiers_;
            bitField0_ = (bitField0_ & ~0x00000001);
            tiersBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getTiersFieldBuilder() : null;
          } else {
            tiersBuilder_.addAllMessages(other.tiers_);
          }
        }
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
      alluxio.grpc.TieredIdentity parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.TieredIdentity) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.LocalityTier> tiers_ =
      java.util.Collections.emptyList();
    private void ensureTiersIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        tiers_ = new java.util.ArrayList<alluxio.grpc.LocalityTier>(tiers_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.LocalityTier, alluxio.grpc.LocalityTier.Builder, alluxio.grpc.LocalityTierOrBuilder> tiersBuilder_;

    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public java.util.List<alluxio.grpc.LocalityTier> getTiersList() {
      if (tiersBuilder_ == null) {
        return java.util.Collections.unmodifiableList(tiers_);
      } else {
        return tiersBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public int getTiersCount() {
      if (tiersBuilder_ == null) {
        return tiers_.size();
      } else {
        return tiersBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public alluxio.grpc.LocalityTier getTiers(int index) {
      if (tiersBuilder_ == null) {
        return tiers_.get(index);
      } else {
        return tiersBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder setTiers(
        int index, alluxio.grpc.LocalityTier value) {
      if (tiersBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTiersIsMutable();
        tiers_.set(index, value);
        onChanged();
      } else {
        tiersBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder setTiers(
        int index, alluxio.grpc.LocalityTier.Builder builderForValue) {
      if (tiersBuilder_ == null) {
        ensureTiersIsMutable();
        tiers_.set(index, builderForValue.build());
        onChanged();
      } else {
        tiersBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder addTiers(alluxio.grpc.LocalityTier value) {
      if (tiersBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTiersIsMutable();
        tiers_.add(value);
        onChanged();
      } else {
        tiersBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder addTiers(
        int index, alluxio.grpc.LocalityTier value) {
      if (tiersBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTiersIsMutable();
        tiers_.add(index, value);
        onChanged();
      } else {
        tiersBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder addTiers(
        alluxio.grpc.LocalityTier.Builder builderForValue) {
      if (tiersBuilder_ == null) {
        ensureTiersIsMutable();
        tiers_.add(builderForValue.build());
        onChanged();
      } else {
        tiersBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder addTiers(
        int index, alluxio.grpc.LocalityTier.Builder builderForValue) {
      if (tiersBuilder_ == null) {
        ensureTiersIsMutable();
        tiers_.add(index, builderForValue.build());
        onChanged();
      } else {
        tiersBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder addAllTiers(
        java.lang.Iterable<? extends alluxio.grpc.LocalityTier> values) {
      if (tiersBuilder_ == null) {
        ensureTiersIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, tiers_);
        onChanged();
      } else {
        tiersBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder clearTiers() {
      if (tiersBuilder_ == null) {
        tiers_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        tiersBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public Builder removeTiers(int index) {
      if (tiersBuilder_ == null) {
        ensureTiersIsMutable();
        tiers_.remove(index);
        onChanged();
      } else {
        tiersBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public alluxio.grpc.LocalityTier.Builder getTiersBuilder(
        int index) {
      return getTiersFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public alluxio.grpc.LocalityTierOrBuilder getTiersOrBuilder(
        int index) {
      if (tiersBuilder_ == null) {
        return tiers_.get(index);  } else {
        return tiersBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.LocalityTierOrBuilder> 
         getTiersOrBuilderList() {
      if (tiersBuilder_ != null) {
        return tiersBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(tiers_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public alluxio.grpc.LocalityTier.Builder addTiersBuilder() {
      return getTiersFieldBuilder().addBuilder(
          alluxio.grpc.LocalityTier.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public alluxio.grpc.LocalityTier.Builder addTiersBuilder(
        int index) {
      return getTiersFieldBuilder().addBuilder(
          index, alluxio.grpc.LocalityTier.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.LocalityTier tiers = 1;</code>
     */
    public java.util.List<alluxio.grpc.LocalityTier.Builder> 
         getTiersBuilderList() {
      return getTiersFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.LocalityTier, alluxio.grpc.LocalityTier.Builder, alluxio.grpc.LocalityTierOrBuilder> 
        getTiersFieldBuilder() {
      if (tiersBuilder_ == null) {
        tiersBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.LocalityTier, alluxio.grpc.LocalityTier.Builder, alluxio.grpc.LocalityTierOrBuilder>(
                tiers_,
                ((bitField0_ & 0x00000001) == 0x00000001),
                getParentForChildren(),
                isClean());
        tiers_ = null;
      }
      return tiersBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.TieredIdentity)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.TieredIdentity)
  private static final alluxio.grpc.TieredIdentity DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.TieredIdentity();
  }

  public static alluxio.grpc.TieredIdentity getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<TieredIdentity>
      PARSER = new com.google.protobuf.AbstractParser<TieredIdentity>() {
    public TieredIdentity parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new TieredIdentity(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<TieredIdentity> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TieredIdentity> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.TieredIdentity getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

