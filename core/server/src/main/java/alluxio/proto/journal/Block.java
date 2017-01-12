// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: block.proto

package alluxio.proto.journal;

public final class Block {
  private Block() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface BlockContainerIdGeneratorEntryOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional int64 next_container_id = 1;
    /**
     * <code>optional int64 next_container_id = 1;</code>
     */
    boolean hasNextContainerId();
    /**
     * <code>optional int64 next_container_id = 1;</code>
     */
    long getNextContainerId();
  }
  /**
   * Protobuf type {@code alluxio.proto.journal.BlockContainerIdGeneratorEntry}
   *
   * <pre>
   * next available id: 2
   * </pre>
   */
  public static final class BlockContainerIdGeneratorEntry extends
      com.google.protobuf.GeneratedMessage
      implements BlockContainerIdGeneratorEntryOrBuilder {
    // Use BlockContainerIdGeneratorEntry.newBuilder() to construct.
    private BlockContainerIdGeneratorEntry(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private BlockContainerIdGeneratorEntry(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final BlockContainerIdGeneratorEntry defaultInstance;
    public static BlockContainerIdGeneratorEntry getDefaultInstance() {
      return defaultInstance;
    }

    public BlockContainerIdGeneratorEntry getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private BlockContainerIdGeneratorEntry(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
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
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              nextContainerId_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.class, alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.Builder.class);
    }

    public static com.google.protobuf.Parser<BlockContainerIdGeneratorEntry> PARSER =
        new com.google.protobuf.AbstractParser<BlockContainerIdGeneratorEntry>() {
      public BlockContainerIdGeneratorEntry parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new BlockContainerIdGeneratorEntry(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<BlockContainerIdGeneratorEntry> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional int64 next_container_id = 1;
    public static final int NEXT_CONTAINER_ID_FIELD_NUMBER = 1;
    private long nextContainerId_;
    /**
     * <code>optional int64 next_container_id = 1;</code>
     */
    public boolean hasNextContainerId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 next_container_id = 1;</code>
     */
    public long getNextContainerId() {
      return nextContainerId_;
    }

    private void initFields() {
      nextContainerId_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, nextContainerId_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, nextContainerId_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code alluxio.proto.journal.BlockContainerIdGeneratorEntry}
     *
     * <pre>
     * next available id: 2
     * </pre>
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements alluxio.proto.journal.Block.BlockContainerIdGeneratorEntryOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.class, alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.Builder.class);
      }

      // Construct using alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        nextContainerId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor;
      }

      public alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry getDefaultInstanceForType() {
        return alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.getDefaultInstance();
      }

      public alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry build() {
        alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry buildPartial() {
        alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry result = new alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.nextContainerId_ = nextContainerId_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry) {
          return mergeFrom((alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry other) {
        if (other == alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry.getDefaultInstance()) return this;
        if (other.hasNextContainerId()) {
          setNextContainerId(other.getNextContainerId());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional int64 next_container_id = 1;
      private long nextContainerId_ ;
      /**
       * <code>optional int64 next_container_id = 1;</code>
       */
      public boolean hasNextContainerId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional int64 next_container_id = 1;</code>
       */
      public long getNextContainerId() {
        return nextContainerId_;
      }
      /**
       * <code>optional int64 next_container_id = 1;</code>
       */
      public Builder setNextContainerId(long value) {
        bitField0_ |= 0x00000001;
        nextContainerId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 next_container_id = 1;</code>
       */
      public Builder clearNextContainerId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        nextContainerId_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.journal.BlockContainerIdGeneratorEntry)
    }

    static {
      defaultInstance = new BlockContainerIdGeneratorEntry(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.journal.BlockContainerIdGeneratorEntry)
  }

  public interface BlockInfoEntryOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional int64 block_id = 1;
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    boolean hasBlockId();
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    long getBlockId();

    // optional int64 length = 2;
    /**
     * <code>optional int64 length = 2;</code>
     */
    boolean hasLength();
    /**
     * <code>optional int64 length = 2;</code>
     */
    long getLength();
  }
  /**
   * Protobuf type {@code alluxio.proto.journal.BlockInfoEntry}
   *
   * <pre>
   * next available id: 3
   * </pre>
   */
  public static final class BlockInfoEntry extends
      com.google.protobuf.GeneratedMessage
      implements BlockInfoEntryOrBuilder {
    // Use BlockInfoEntry.newBuilder() to construct.
    private BlockInfoEntry(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private BlockInfoEntry(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final BlockInfoEntry defaultInstance;
    public static BlockInfoEntry getDefaultInstance() {
      return defaultInstance;
    }

    public BlockInfoEntry getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private BlockInfoEntry(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
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
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              blockId_ = input.readInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              length_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockInfoEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.proto.journal.Block.BlockInfoEntry.class, alluxio.proto.journal.Block.BlockInfoEntry.Builder.class);
    }

    public static com.google.protobuf.Parser<BlockInfoEntry> PARSER =
        new com.google.protobuf.AbstractParser<BlockInfoEntry>() {
      public BlockInfoEntry parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new BlockInfoEntry(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<BlockInfoEntry> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional int64 block_id = 1;
    public static final int BLOCK_ID_FIELD_NUMBER = 1;
    private long blockId_;
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public boolean hasBlockId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 block_id = 1;</code>
     */
    public long getBlockId() {
      return blockId_;
    }

    // optional int64 length = 2;
    public static final int LENGTH_FIELD_NUMBER = 2;
    private long length_;
    /**
     * <code>optional int64 length = 2;</code>
     */
    public boolean hasLength() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int64 length = 2;</code>
     */
    public long getLength() {
      return length_;
    }

    private void initFields() {
      blockId_ = 0L;
      length_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, blockId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt64(2, length_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, blockId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, length_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static alluxio.proto.journal.Block.BlockInfoEntry parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(alluxio.proto.journal.Block.BlockInfoEntry prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code alluxio.proto.journal.BlockInfoEntry}
     *
     * <pre>
     * next available id: 3
     * </pre>
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements alluxio.proto.journal.Block.BlockInfoEntryOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockInfoEntry_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                alluxio.proto.journal.Block.BlockInfoEntry.class, alluxio.proto.journal.Block.BlockInfoEntry.Builder.class);
      }

      // Construct using alluxio.proto.journal.Block.BlockInfoEntry.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        blockId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        length_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return alluxio.proto.journal.Block.internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor;
      }

      public alluxio.proto.journal.Block.BlockInfoEntry getDefaultInstanceForType() {
        return alluxio.proto.journal.Block.BlockInfoEntry.getDefaultInstance();
      }

      public alluxio.proto.journal.Block.BlockInfoEntry build() {
        alluxio.proto.journal.Block.BlockInfoEntry result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public alluxio.proto.journal.Block.BlockInfoEntry buildPartial() {
        alluxio.proto.journal.Block.BlockInfoEntry result = new alluxio.proto.journal.Block.BlockInfoEntry(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.blockId_ = blockId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.length_ = length_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof alluxio.proto.journal.Block.BlockInfoEntry) {
          return mergeFrom((alluxio.proto.journal.Block.BlockInfoEntry)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(alluxio.proto.journal.Block.BlockInfoEntry other) {
        if (other == alluxio.proto.journal.Block.BlockInfoEntry.getDefaultInstance()) return this;
        if (other.hasBlockId()) {
          setBlockId(other.getBlockId());
        }
        if (other.hasLength()) {
          setLength(other.getLength());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        alluxio.proto.journal.Block.BlockInfoEntry parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (alluxio.proto.journal.Block.BlockInfoEntry) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional int64 block_id = 1;
      private long blockId_ ;
      /**
       * <code>optional int64 block_id = 1;</code>
       */
      public boolean hasBlockId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional int64 block_id = 1;</code>
       */
      public long getBlockId() {
        return blockId_;
      }
      /**
       * <code>optional int64 block_id = 1;</code>
       */
      public Builder setBlockId(long value) {
        bitField0_ |= 0x00000001;
        blockId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 block_id = 1;</code>
       */
      public Builder clearBlockId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        blockId_ = 0L;
        onChanged();
        return this;
      }

      // optional int64 length = 2;
      private long length_ ;
      /**
       * <code>optional int64 length = 2;</code>
       */
      public boolean hasLength() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional int64 length = 2;</code>
       */
      public long getLength() {
        return length_;
      }
      /**
       * <code>optional int64 length = 2;</code>
       */
      public Builder setLength(long value) {
        bitField0_ |= 0x00000002;
        length_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 length = 2;</code>
       */
      public Builder clearLength() {
        bitField0_ = (bitField0_ & ~0x00000002);
        length_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:alluxio.proto.journal.BlockInfoEntry)
    }

    static {
      defaultInstance = new BlockInfoEntry(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:alluxio.proto.journal.BlockInfoEntry)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_alluxio_proto_journal_BlockInfoEntry_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\013block.proto\022\025alluxio.proto.journal\";\n\036" +
      "BlockContainerIdGeneratorEntry\022\031\n\021next_c" +
      "ontainer_id\030\001 \001(\003\"2\n\016BlockInfoEntry\022\020\n\010b" +
      "lock_id\030\001 \001(\003\022\016\n\006length\030\002 \001(\003"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_alluxio_proto_journal_BlockContainerIdGeneratorEntry_descriptor,
              new java.lang.String[] { "NextContainerId", });
          internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_alluxio_proto_journal_BlockInfoEntry_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_alluxio_proto_journal_BlockInfoEntry_descriptor,
              new java.lang.String[] { "BlockId", "Length", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
