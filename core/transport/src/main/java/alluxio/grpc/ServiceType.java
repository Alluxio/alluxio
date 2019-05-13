// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/version.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.version.ServiceType}
 */
public enum ServiceType
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>UNKNOWN_SERVICE = 0;</code>
   */
  UNKNOWN_SERVICE(0),
  /**
   * <code>FILE_SYSTEM_MASTER_CLIENT_SERVICE = 1;</code>
   */
  FILE_SYSTEM_MASTER_CLIENT_SERVICE(1),
  /**
   * <code>FILE_SYSTEM_MASTER_WORKER_SERVICE = 2;</code>
   */
  FILE_SYSTEM_MASTER_WORKER_SERVICE(2),
  /**
   * <code>FILE_SYSTEM_MASTER_JOB_SERVICE = 3;</code>
   */
  FILE_SYSTEM_MASTER_JOB_SERVICE(3),
  /**
   * <code>BLOCK_MASTER_CLIENT_SERVICE = 4;</code>
   */
  BLOCK_MASTER_CLIENT_SERVICE(4),
  /**
   * <code>BLOCK_MASTER_WORKER_SERVICE = 5;</code>
   */
  BLOCK_MASTER_WORKER_SERVICE(5),
  /**
   * <code>META_MASTER_CONFIG_SERVICE = 6;</code>
   */
  META_MASTER_CONFIG_SERVICE(6),
  /**
   * <code>META_MASTER_CLIENT_SERVICE = 7;</code>
   */
  META_MASTER_CLIENT_SERVICE(7),
  /**
   * <code>META_MASTER_MASTER_SERVICE = 8;</code>
   */
  META_MASTER_MASTER_SERVICE(8),
  /**
   * <code>METRICS_MASTER_CLIENT_SERVICE = 9;</code>
   */
  METRICS_MASTER_CLIENT_SERVICE(9),
  /**
   * <code>JOB_MASTER_CLIENT_SERVICE = 10;</code>
   */
  JOB_MASTER_CLIENT_SERVICE(10),
  /**
   * <code>JOB_MASTER_WORKER_SERVICE = 11;</code>
   */
  JOB_MASTER_WORKER_SERVICE(11),
  /**
   * <code>FILE_SYSTEM_WORKER_WORKER_SERVICE = 12;</code>
   */
  FILE_SYSTEM_WORKER_WORKER_SERVICE(12),
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>PRIVILEGE_MASTER_CLIENT_SERVICE = 1001;</code>
   */
  PRIVILEGE_MASTER_CLIENT_SERVICE(1001),
  ;

  /**
   * <code>UNKNOWN_SERVICE = 0;</code>
   */
  public static final int UNKNOWN_SERVICE_VALUE = 0;
  /**
   * <code>FILE_SYSTEM_MASTER_CLIENT_SERVICE = 1;</code>
   */
  public static final int FILE_SYSTEM_MASTER_CLIENT_SERVICE_VALUE = 1;
  /**
   * <code>FILE_SYSTEM_MASTER_WORKER_SERVICE = 2;</code>
   */
  public static final int FILE_SYSTEM_MASTER_WORKER_SERVICE_VALUE = 2;
  /**
   * <code>FILE_SYSTEM_MASTER_JOB_SERVICE = 3;</code>
   */
  public static final int FILE_SYSTEM_MASTER_JOB_SERVICE_VALUE = 3;
  /**
   * <code>BLOCK_MASTER_CLIENT_SERVICE = 4;</code>
   */
  public static final int BLOCK_MASTER_CLIENT_SERVICE_VALUE = 4;
  /**
   * <code>BLOCK_MASTER_WORKER_SERVICE = 5;</code>
   */
  public static final int BLOCK_MASTER_WORKER_SERVICE_VALUE = 5;
  /**
   * <code>META_MASTER_CONFIG_SERVICE = 6;</code>
   */
  public static final int META_MASTER_CONFIG_SERVICE_VALUE = 6;
  /**
   * <code>META_MASTER_CLIENT_SERVICE = 7;</code>
   */
  public static final int META_MASTER_CLIENT_SERVICE_VALUE = 7;
  /**
   * <code>META_MASTER_MASTER_SERVICE = 8;</code>
   */
  public static final int META_MASTER_MASTER_SERVICE_VALUE = 8;
  /**
   * <code>METRICS_MASTER_CLIENT_SERVICE = 9;</code>
   */
  public static final int METRICS_MASTER_CLIENT_SERVICE_VALUE = 9;
  /**
   * <code>JOB_MASTER_CLIENT_SERVICE = 10;</code>
   */
  public static final int JOB_MASTER_CLIENT_SERVICE_VALUE = 10;
  /**
   * <code>JOB_MASTER_WORKER_SERVICE = 11;</code>
   */
  public static final int JOB_MASTER_WORKER_SERVICE_VALUE = 11;
  /**
   * <code>FILE_SYSTEM_WORKER_WORKER_SERVICE = 12;</code>
   */
  public static final int FILE_SYSTEM_WORKER_WORKER_SERVICE_VALUE = 12;
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>PRIVILEGE_MASTER_CLIENT_SERVICE = 1001;</code>
   */
  public static final int PRIVILEGE_MASTER_CLIENT_SERVICE_VALUE = 1001;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static ServiceType valueOf(int value) {
    return forNumber(value);
  }

  public static ServiceType forNumber(int value) {
    switch (value) {
      case 0: return UNKNOWN_SERVICE;
      case 1: return FILE_SYSTEM_MASTER_CLIENT_SERVICE;
      case 2: return FILE_SYSTEM_MASTER_WORKER_SERVICE;
      case 3: return FILE_SYSTEM_MASTER_JOB_SERVICE;
      case 4: return BLOCK_MASTER_CLIENT_SERVICE;
      case 5: return BLOCK_MASTER_WORKER_SERVICE;
      case 6: return META_MASTER_CONFIG_SERVICE;
      case 7: return META_MASTER_CLIENT_SERVICE;
      case 8: return META_MASTER_MASTER_SERVICE;
      case 9: return METRICS_MASTER_CLIENT_SERVICE;
      case 10: return JOB_MASTER_CLIENT_SERVICE;
      case 11: return JOB_MASTER_WORKER_SERVICE;
      case 12: return FILE_SYSTEM_WORKER_WORKER_SERVICE;
      case 1001: return PRIVILEGE_MASTER_CLIENT_SERVICE;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<ServiceType>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      ServiceType> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<ServiceType>() {
          public ServiceType findValueByNumber(int number) {
            return ServiceType.forNumber(number);
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
    return alluxio.grpc.VersionProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final ServiceType[] VALUES = values();

  public static ServiceType valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private ServiceType(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.version.ServiceType)
}

