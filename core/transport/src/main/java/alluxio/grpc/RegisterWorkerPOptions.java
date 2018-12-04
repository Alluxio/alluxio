// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: block_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.RegisterWorkerPOptions}
 */
public  final class RegisterWorkerPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.RegisterWorkerPOptions)
    RegisterWorkerPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use RegisterWorkerPOptions.newBuilder() to construct.
  private RegisterWorkerPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private RegisterWorkerPOptions() {
    configs_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private RegisterWorkerPOptions(
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
              configs_ = new java.util.ArrayList<alluxio.grpc.ConfigProperty>();
              mutable_bitField0_ |= 0x00000001;
            }
            configs_.add(
                input.readMessage(alluxio.grpc.ConfigProperty.PARSER, extensionRegistry));
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
        configs_ = java.util.Collections.unmodifiableList(configs_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_RegisterWorkerPOptions_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_RegisterWorkerPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.RegisterWorkerPOptions.class, alluxio.grpc.RegisterWorkerPOptions.Builder.class);
  }

  public static final int CONFIGS_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.ConfigProperty> configs_;
  /**
   * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
   */
  public java.util.List<alluxio.grpc.ConfigProperty> getConfigsList() {
    return configs_;
  }
  /**
   * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
   */
  public java.util.List<? extends alluxio.grpc.ConfigPropertyOrBuilder> 
      getConfigsOrBuilderList() {
    return configs_;
  }
  /**
   * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
   */
  public int getConfigsCount() {
    return configs_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
   */
  public alluxio.grpc.ConfigProperty getConfigs(int index) {
    return configs_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
   */
  public alluxio.grpc.ConfigPropertyOrBuilder getConfigsOrBuilder(
      int index) {
    return configs_.get(index);
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
    for (int i = 0; i < configs_.size(); i++) {
      output.writeMessage(1, configs_.get(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < configs_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, configs_.get(i));
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
    if (!(obj instanceof alluxio.grpc.RegisterWorkerPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.RegisterWorkerPOptions other = (alluxio.grpc.RegisterWorkerPOptions) obj;

    boolean result = true;
    result = result && getConfigsList()
        .equals(other.getConfigsList());
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
    if (getConfigsCount() > 0) {
      hash = (37 * hash) + CONFIGS_FIELD_NUMBER;
      hash = (53 * hash) + getConfigsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.RegisterWorkerPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.RegisterWorkerPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.RegisterWorkerPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.RegisterWorkerPOptions)
      alluxio.grpc.RegisterWorkerPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_RegisterWorkerPOptions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_RegisterWorkerPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.RegisterWorkerPOptions.class, alluxio.grpc.RegisterWorkerPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.RegisterWorkerPOptions.newBuilder()
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
        getConfigsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      if (configsBuilder_ == null) {
        configs_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        configsBuilder_.clear();
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.BlockMasterProto.internal_static_alluxio_grpc_RegisterWorkerPOptions_descriptor;
    }

    public alluxio.grpc.RegisterWorkerPOptions getDefaultInstanceForType() {
      return alluxio.grpc.RegisterWorkerPOptions.getDefaultInstance();
    }

    public alluxio.grpc.RegisterWorkerPOptions build() {
      alluxio.grpc.RegisterWorkerPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.RegisterWorkerPOptions buildPartial() {
      alluxio.grpc.RegisterWorkerPOptions result = new alluxio.grpc.RegisterWorkerPOptions(this);
      int from_bitField0_ = bitField0_;
      if (configsBuilder_ == null) {
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          configs_ = java.util.Collections.unmodifiableList(configs_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.configs_ = configs_;
      } else {
        result.configs_ = configsBuilder_.build();
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
      if (other instanceof alluxio.grpc.RegisterWorkerPOptions) {
        return mergeFrom((alluxio.grpc.RegisterWorkerPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.RegisterWorkerPOptions other) {
      if (other == alluxio.grpc.RegisterWorkerPOptions.getDefaultInstance()) return this;
      if (configsBuilder_ == null) {
        if (!other.configs_.isEmpty()) {
          if (configs_.isEmpty()) {
            configs_ = other.configs_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureConfigsIsMutable();
            configs_.addAll(other.configs_);
          }
          onChanged();
        }
      } else {
        if (!other.configs_.isEmpty()) {
          if (configsBuilder_.isEmpty()) {
            configsBuilder_.dispose();
            configsBuilder_ = null;
            configs_ = other.configs_;
            bitField0_ = (bitField0_ & ~0x00000001);
            configsBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getConfigsFieldBuilder() : null;
          } else {
            configsBuilder_.addAllMessages(other.configs_);
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
      alluxio.grpc.RegisterWorkerPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.RegisterWorkerPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.ConfigProperty> configs_ =
      java.util.Collections.emptyList();
    private void ensureConfigsIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        configs_ = new java.util.ArrayList<alluxio.grpc.ConfigProperty>(configs_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.ConfigProperty, alluxio.grpc.ConfigProperty.Builder, alluxio.grpc.ConfigPropertyOrBuilder> configsBuilder_;

    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public java.util.List<alluxio.grpc.ConfigProperty> getConfigsList() {
      if (configsBuilder_ == null) {
        return java.util.Collections.unmodifiableList(configs_);
      } else {
        return configsBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public int getConfigsCount() {
      if (configsBuilder_ == null) {
        return configs_.size();
      } else {
        return configsBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public alluxio.grpc.ConfigProperty getConfigs(int index) {
      if (configsBuilder_ == null) {
        return configs_.get(index);
      } else {
        return configsBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder setConfigs(
        int index, alluxio.grpc.ConfigProperty value) {
      if (configsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureConfigsIsMutable();
        configs_.set(index, value);
        onChanged();
      } else {
        configsBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder setConfigs(
        int index, alluxio.grpc.ConfigProperty.Builder builderForValue) {
      if (configsBuilder_ == null) {
        ensureConfigsIsMutable();
        configs_.set(index, builderForValue.build());
        onChanged();
      } else {
        configsBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder addConfigs(alluxio.grpc.ConfigProperty value) {
      if (configsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureConfigsIsMutable();
        configs_.add(value);
        onChanged();
      } else {
        configsBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder addConfigs(
        int index, alluxio.grpc.ConfigProperty value) {
      if (configsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureConfigsIsMutable();
        configs_.add(index, value);
        onChanged();
      } else {
        configsBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder addConfigs(
        alluxio.grpc.ConfigProperty.Builder builderForValue) {
      if (configsBuilder_ == null) {
        ensureConfigsIsMutable();
        configs_.add(builderForValue.build());
        onChanged();
      } else {
        configsBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder addConfigs(
        int index, alluxio.grpc.ConfigProperty.Builder builderForValue) {
      if (configsBuilder_ == null) {
        ensureConfigsIsMutable();
        configs_.add(index, builderForValue.build());
        onChanged();
      } else {
        configsBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder addAllConfigs(
        java.lang.Iterable<? extends alluxio.grpc.ConfigProperty> values) {
      if (configsBuilder_ == null) {
        ensureConfigsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, configs_);
        onChanged();
      } else {
        configsBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder clearConfigs() {
      if (configsBuilder_ == null) {
        configs_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        configsBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public Builder removeConfigs(int index) {
      if (configsBuilder_ == null) {
        ensureConfigsIsMutable();
        configs_.remove(index);
        onChanged();
      } else {
        configsBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public alluxio.grpc.ConfigProperty.Builder getConfigsBuilder(
        int index) {
      return getConfigsFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public alluxio.grpc.ConfigPropertyOrBuilder getConfigsOrBuilder(
        int index) {
      if (configsBuilder_ == null) {
        return configs_.get(index);  } else {
        return configsBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.ConfigPropertyOrBuilder> 
         getConfigsOrBuilderList() {
      if (configsBuilder_ != null) {
        return configsBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(configs_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public alluxio.grpc.ConfigProperty.Builder addConfigsBuilder() {
      return getConfigsFieldBuilder().addBuilder(
          alluxio.grpc.ConfigProperty.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public alluxio.grpc.ConfigProperty.Builder addConfigsBuilder(
        int index) {
      return getConfigsFieldBuilder().addBuilder(
          index, alluxio.grpc.ConfigProperty.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.ConfigProperty configs = 1;</code>
     */
    public java.util.List<alluxio.grpc.ConfigProperty.Builder> 
         getConfigsBuilderList() {
      return getConfigsFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.ConfigProperty, alluxio.grpc.ConfigProperty.Builder, alluxio.grpc.ConfigPropertyOrBuilder> 
        getConfigsFieldBuilder() {
      if (configsBuilder_ == null) {
        configsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.ConfigProperty, alluxio.grpc.ConfigProperty.Builder, alluxio.grpc.ConfigPropertyOrBuilder>(
                configs_,
                ((bitField0_ & 0x00000001) == 0x00000001),
                getParentForChildren(),
                isClean());
        configs_ = null;
      }
      return configsBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.RegisterWorkerPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.RegisterWorkerPOptions)
  private static final alluxio.grpc.RegisterWorkerPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.RegisterWorkerPOptions();
  }

  public static alluxio.grpc.RegisterWorkerPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<RegisterWorkerPOptions>
      PARSER = new com.google.protobuf.AbstractParser<RegisterWorkerPOptions>() {
    public RegisterWorkerPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new RegisterWorkerPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<RegisterWorkerPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<RegisterWorkerPOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.RegisterWorkerPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

