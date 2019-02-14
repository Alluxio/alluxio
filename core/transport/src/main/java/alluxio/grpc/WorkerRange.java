// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.block.WorkerRange}
 */
public enum WorkerRange
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>ALL = 1;</code>
   */
  ALL(1),
  /**
   * <code>LIVE = 2;</code>
   */
  LIVE(2),
  /**
   * <code>LOST = 3;</code>
   */
  LOST(3),
  /**
   * <code>SPECIFIED = 4;</code>
   */
  SPECIFIED(4),
  ;

  /**
   * <code>ALL = 1;</code>
   */
  public static final int ALL_VALUE = 1;
  /**
   * <code>LIVE = 2;</code>
   */
  public static final int LIVE_VALUE = 2;
  /**
   * <code>LOST = 3;</code>
   */
  public static final int LOST_VALUE = 3;
  /**
   * <code>SPECIFIED = 4;</code>
   */
  public static final int SPECIFIED_VALUE = 4;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static WorkerRange valueOf(int value) {
    return forNumber(value);
  }

  public static WorkerRange forNumber(int value) {
    switch (value) {
      case 1: return ALL;
      case 2: return LIVE;
      case 3: return LOST;
      case 4: return SPECIFIED;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<WorkerRange>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      WorkerRange> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<WorkerRange>() {
          public WorkerRange findValueByNumber(int number) {
            return WorkerRange.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return alluxio.grpc.BlockMasterProto.getDescriptor().getEnumTypes().get(1);
  }

  private static final WorkerRange[] VALUES = values();

  public static WorkerRange valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private WorkerRange(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.block.WorkerRange)
}

