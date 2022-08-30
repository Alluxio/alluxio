// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.meta.InconsistentProperties}
 */
public final class InconsistentProperties extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.meta.InconsistentProperties)
    InconsistentPropertiesOrBuilder {
private static final long serialVersionUID = 0L;
  // Use InconsistentProperties.newBuilder() to construct.
  private InconsistentProperties(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private InconsistentProperties() {
    properties_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new InconsistentProperties();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private InconsistentProperties(
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
              properties_ = new java.util.ArrayList<alluxio.grpc.InconsistentProperty>();
              mutable_bitField0_ |= 0x00000001;
            }
            properties_.add(
                input.readMessage(alluxio.grpc.InconsistentProperty.PARSER, extensionRegistry));
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
        properties_ = java.util.Collections.unmodifiableList(properties_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_InconsistentProperties_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_InconsistentProperties_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.InconsistentProperties.class, alluxio.grpc.InconsistentProperties.Builder.class);
  }

  public static final int PROPERTIES_FIELD_NUMBER = 1;
  private java.util.List<alluxio.grpc.InconsistentProperty> properties_;
  /**
   * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
   */
  @java.lang.Override
  public java.util.List<alluxio.grpc.InconsistentProperty> getPropertiesList() {
    return properties_;
  }
  /**
   * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
   */
  @java.lang.Override
  public java.util.List<? extends alluxio.grpc.InconsistentPropertyOrBuilder> 
      getPropertiesOrBuilderList() {
    return properties_;
  }
  /**
   * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
   */
  @java.lang.Override
  public int getPropertiesCount() {
    return properties_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.InconsistentProperty getProperties(int index) {
    return properties_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
   */
  @java.lang.Override
  public alluxio.grpc.InconsistentPropertyOrBuilder getPropertiesOrBuilder(
      int index) {
    return properties_.get(index);
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
    for (int i = 0; i < properties_.size(); i++) {
      output.writeMessage(1, properties_.get(i));
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    for (int i = 0; i < properties_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, properties_.get(i));
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
    if (!(obj instanceof alluxio.grpc.InconsistentProperties)) {
      return super.equals(obj);
    }
    alluxio.grpc.InconsistentProperties other = (alluxio.grpc.InconsistentProperties) obj;

    if (!getPropertiesList()
        .equals(other.getPropertiesList())) return false;
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
    if (getPropertiesCount() > 0) {
      hash = (37 * hash) + PROPERTIES_FIELD_NUMBER;
      hash = (53 * hash) + getPropertiesList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.InconsistentProperties parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.InconsistentProperties parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.InconsistentProperties parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.InconsistentProperties parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.InconsistentProperties prototype) {
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
   * Protobuf type {@code alluxio.grpc.meta.InconsistentProperties}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.meta.InconsistentProperties)
      alluxio.grpc.InconsistentPropertiesOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_InconsistentProperties_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_InconsistentProperties_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.InconsistentProperties.class, alluxio.grpc.InconsistentProperties.Builder.class);
    }

    // Construct using alluxio.grpc.InconsistentProperties.newBuilder()
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
        getPropertiesFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      if (propertiesBuilder_ == null) {
        properties_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        propertiesBuilder_.clear();
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.MetaMasterProto.internal_static_alluxio_grpc_meta_InconsistentProperties_descriptor;
    }

    @java.lang.Override
    public alluxio.grpc.InconsistentProperties getDefaultInstanceForType() {
      return alluxio.grpc.InconsistentProperties.getDefaultInstance();
    }

    @java.lang.Override
    public alluxio.grpc.InconsistentProperties build() {
      alluxio.grpc.InconsistentProperties result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public alluxio.grpc.InconsistentProperties buildPartial() {
      alluxio.grpc.InconsistentProperties result = new alluxio.grpc.InconsistentProperties(this);
      int from_bitField0_ = bitField0_;
      if (propertiesBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0)) {
          properties_ = java.util.Collections.unmodifiableList(properties_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.properties_ = properties_;
      } else {
        result.properties_ = propertiesBuilder_.build();
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
      if (other instanceof alluxio.grpc.InconsistentProperties) {
        return mergeFrom((alluxio.grpc.InconsistentProperties)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.InconsistentProperties other) {
      if (other == alluxio.grpc.InconsistentProperties.getDefaultInstance()) return this;
      if (propertiesBuilder_ == null) {
        if (!other.properties_.isEmpty()) {
          if (properties_.isEmpty()) {
            properties_ = other.properties_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensurePropertiesIsMutable();
            properties_.addAll(other.properties_);
          }
          onChanged();
        }
      } else {
        if (!other.properties_.isEmpty()) {
          if (propertiesBuilder_.isEmpty()) {
            propertiesBuilder_.dispose();
            propertiesBuilder_ = null;
            properties_ = other.properties_;
            bitField0_ = (bitField0_ & ~0x00000001);
            propertiesBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getPropertiesFieldBuilder() : null;
          } else {
            propertiesBuilder_.addAllMessages(other.properties_);
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
      alluxio.grpc.InconsistentProperties parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.InconsistentProperties) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.util.List<alluxio.grpc.InconsistentProperty> properties_ =
      java.util.Collections.emptyList();
    private void ensurePropertiesIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        properties_ = new java.util.ArrayList<alluxio.grpc.InconsistentProperty>(properties_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.InconsistentProperty, alluxio.grpc.InconsistentProperty.Builder, alluxio.grpc.InconsistentPropertyOrBuilder> propertiesBuilder_;

    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public java.util.List<alluxio.grpc.InconsistentProperty> getPropertiesList() {
      if (propertiesBuilder_ == null) {
        return java.util.Collections.unmodifiableList(properties_);
      } else {
        return propertiesBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public int getPropertiesCount() {
      if (propertiesBuilder_ == null) {
        return properties_.size();
      } else {
        return propertiesBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public alluxio.grpc.InconsistentProperty getProperties(int index) {
      if (propertiesBuilder_ == null) {
        return properties_.get(index);
      } else {
        return propertiesBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder setProperties(
        int index, alluxio.grpc.InconsistentProperty value) {
      if (propertiesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePropertiesIsMutable();
        properties_.set(index, value);
        onChanged();
      } else {
        propertiesBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder setProperties(
        int index, alluxio.grpc.InconsistentProperty.Builder builderForValue) {
      if (propertiesBuilder_ == null) {
        ensurePropertiesIsMutable();
        properties_.set(index, builderForValue.build());
        onChanged();
      } else {
        propertiesBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder addProperties(alluxio.grpc.InconsistentProperty value) {
      if (propertiesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePropertiesIsMutable();
        properties_.add(value);
        onChanged();
      } else {
        propertiesBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder addProperties(
        int index, alluxio.grpc.InconsistentProperty value) {
      if (propertiesBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensurePropertiesIsMutable();
        properties_.add(index, value);
        onChanged();
      } else {
        propertiesBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder addProperties(
        alluxio.grpc.InconsistentProperty.Builder builderForValue) {
      if (propertiesBuilder_ == null) {
        ensurePropertiesIsMutable();
        properties_.add(builderForValue.build());
        onChanged();
      } else {
        propertiesBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder addProperties(
        int index, alluxio.grpc.InconsistentProperty.Builder builderForValue) {
      if (propertiesBuilder_ == null) {
        ensurePropertiesIsMutable();
        properties_.add(index, builderForValue.build());
        onChanged();
      } else {
        propertiesBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder addAllProperties(
        java.lang.Iterable<? extends alluxio.grpc.InconsistentProperty> values) {
      if (propertiesBuilder_ == null) {
        ensurePropertiesIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, properties_);
        onChanged();
      } else {
        propertiesBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder clearProperties() {
      if (propertiesBuilder_ == null) {
        properties_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        propertiesBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public Builder removeProperties(int index) {
      if (propertiesBuilder_ == null) {
        ensurePropertiesIsMutable();
        properties_.remove(index);
        onChanged();
      } else {
        propertiesBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public alluxio.grpc.InconsistentProperty.Builder getPropertiesBuilder(
        int index) {
      return getPropertiesFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public alluxio.grpc.InconsistentPropertyOrBuilder getPropertiesOrBuilder(
        int index) {
      if (propertiesBuilder_ == null) {
        return properties_.get(index);  } else {
        return propertiesBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public java.util.List<? extends alluxio.grpc.InconsistentPropertyOrBuilder> 
         getPropertiesOrBuilderList() {
      if (propertiesBuilder_ != null) {
        return propertiesBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(properties_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public alluxio.grpc.InconsistentProperty.Builder addPropertiesBuilder() {
      return getPropertiesFieldBuilder().addBuilder(
          alluxio.grpc.InconsistentProperty.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public alluxio.grpc.InconsistentProperty.Builder addPropertiesBuilder(
        int index) {
      return getPropertiesFieldBuilder().addBuilder(
          index, alluxio.grpc.InconsistentProperty.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.meta.InconsistentProperty properties = 1;</code>
     */
    public java.util.List<alluxio.grpc.InconsistentProperty.Builder> 
         getPropertiesBuilderList() {
      return getPropertiesFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.InconsistentProperty, alluxio.grpc.InconsistentProperty.Builder, alluxio.grpc.InconsistentPropertyOrBuilder> 
        getPropertiesFieldBuilder() {
      if (propertiesBuilder_ == null) {
        propertiesBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.InconsistentProperty, alluxio.grpc.InconsistentProperty.Builder, alluxio.grpc.InconsistentPropertyOrBuilder>(
                properties_,
                ((bitField0_ & 0x00000001) != 0),
                getParentForChildren(),
                isClean());
        properties_ = null;
      }
      return propertiesBuilder_;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.meta.InconsistentProperties)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.meta.InconsistentProperties)
  private static final alluxio.grpc.InconsistentProperties DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.InconsistentProperties();
  }

  public static alluxio.grpc.InconsistentProperties getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<InconsistentProperties>
      PARSER = new com.google.protobuf.AbstractParser<InconsistentProperties>() {
    @java.lang.Override
    public InconsistentProperties parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new InconsistentProperties(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<InconsistentProperties> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<InconsistentProperties> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public alluxio.grpc.InconsistentProperties getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

