// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: meta_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.ConfigStatus}
 */
public enum ConfigStatus
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>PASSED = 1;</code>
   */
  PASSED(1),
  /**
   * <code>WARN = 2;</code>
   */
  WARN(2),
  /**
   * <code>FAILED = 3;</code>
   */
  FAILED(3),
  ;

  /**
   * <code>PASSED = 1;</code>
   */
  public static final int PASSED_VALUE = 1;
  /**
   * <code>WARN = 2;</code>
   */
  public static final int WARN_VALUE = 2;
  /**
   * <code>FAILED = 3;</code>
   */
  public static final int FAILED_VALUE = 3;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static ConfigStatus valueOf(int value) {
    return forNumber(value);
  }

  public static ConfigStatus forNumber(int value) {
    switch (value) {
      case 1: return PASSED;
      case 2: return WARN;
      case 3: return FAILED;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<ConfigStatus>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      ConfigStatus> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<ConfigStatus>() {
          public ConfigStatus findValueByNumber(int number) {
            return ConfigStatus.forNumber(number);
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
    return alluxio.grpc.MetaMasterProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final ConfigStatus[] VALUES = values();

  public static ConfigStatus valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private ConfigStatus(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.ConfigStatus)
}

