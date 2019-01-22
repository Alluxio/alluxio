// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.ModeBits}
 */
public enum ModeBits
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>NONE = 1;</code>
   */
  NONE(1),
  /**
   * <code>EXECUTE = 2;</code>
   */
  EXECUTE(2),
  /**
   * <code>WRITE = 3;</code>
   */
  WRITE(3),
  /**
   * <code>WRITE_EXECUTE = 4;</code>
   */
  WRITE_EXECUTE(4),
  /**
   * <code>READ = 5;</code>
   */
  READ(5),
  /**
   * <code>READ_EXECUTE = 6;</code>
   */
  READ_EXECUTE(6),
  /**
   * <code>READ_WRITE = 7;</code>
   */
  READ_WRITE(7),
  /**
   * <code>ALL = 8;</code>
   */
  ALL(8),
  ;

  /**
   * <code>NONE = 1;</code>
   */
  public static final int NONE_VALUE = 1;
  /**
   * <code>EXECUTE = 2;</code>
   */
  public static final int EXECUTE_VALUE = 2;
  /**
   * <code>WRITE = 3;</code>
   */
  public static final int WRITE_VALUE = 3;
  /**
   * <code>WRITE_EXECUTE = 4;</code>
   */
  public static final int WRITE_EXECUTE_VALUE = 4;
  /**
   * <code>READ = 5;</code>
   */
  public static final int READ_VALUE = 5;
  /**
   * <code>READ_EXECUTE = 6;</code>
   */
  public static final int READ_EXECUTE_VALUE = 6;
  /**
   * <code>READ_WRITE = 7;</code>
   */
  public static final int READ_WRITE_VALUE = 7;
  /**
   * <code>ALL = 8;</code>
   */
  public static final int ALL_VALUE = 8;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static ModeBits valueOf(int value) {
    return forNumber(value);
  }

  public static ModeBits forNumber(int value) {
    switch (value) {
      case 1: return NONE;
      case 2: return EXECUTE;
      case 3: return WRITE;
      case 4: return WRITE_EXECUTE;
      case 5: return READ;
      case 6: return READ_EXECUTE;
      case 7: return READ_WRITE;
      case 8: return ALL;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<ModeBits>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      ModeBits> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<ModeBits>() {
          public ModeBits findValueByNumber(int number) {
            return ModeBits.forNumber(number);
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
    return alluxio.grpc.CommonProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final ModeBits[] VALUES = values();

  public static ModeBits valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private ModeBits(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.ModeBits)
}

