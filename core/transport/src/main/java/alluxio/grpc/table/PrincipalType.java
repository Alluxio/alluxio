// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf enum {@code alluxio.grpc.table.PrincipalType}
 */
public enum PrincipalType
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>USER = 0;</code>
   */
  USER(0),
  /**
   * <code>ROLE = 1;</code>
   */
  ROLE(1),
  ;

  /**
   * <code>USER = 0;</code>
   */
  public static final int USER_VALUE = 0;
  /**
   * <code>ROLE = 1;</code>
   */
  public static final int ROLE_VALUE = 1;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static PrincipalType valueOf(int value) {
    return forNumber(value);
  }

  public static PrincipalType forNumber(int value) {
    switch (value) {
      case 0: return USER;
      case 1: return ROLE;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<PrincipalType>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      PrincipalType> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<PrincipalType>() {
          public PrincipalType findValueByNumber(int number) {
            return PrincipalType.forNumber(number);
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
    return alluxio.grpc.table.TableMasterProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final PrincipalType[] VALUES = values();

  public static PrincipalType valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private PrincipalType(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.table.PrincipalType)
}

