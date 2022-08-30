// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.file.SetAclAction}
 */
public enum SetAclAction
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>REPLACE = 0;</code>
   */
  REPLACE(0),
  /**
   * <code>MODIFY = 1;</code>
   */
  MODIFY(1),
  /**
   * <code>REMOVE = 2;</code>
   */
  REMOVE(2),
  /**
   * <code>REMOVE_ALL = 3;</code>
   */
  REMOVE_ALL(3),
  /**
   * <code>REMOVE_DEFAULT = 4;</code>
   */
  REMOVE_DEFAULT(4),
  ;

  /**
   * <code>REPLACE = 0;</code>
   */
  public static final int REPLACE_VALUE = 0;
  /**
   * <code>MODIFY = 1;</code>
   */
  public static final int MODIFY_VALUE = 1;
  /**
   * <code>REMOVE = 2;</code>
   */
  public static final int REMOVE_VALUE = 2;
  /**
   * <code>REMOVE_ALL = 3;</code>
   */
  public static final int REMOVE_ALL_VALUE = 3;
  /**
   * <code>REMOVE_DEFAULT = 4;</code>
   */
  public static final int REMOVE_DEFAULT_VALUE = 4;


  public final int getNumber() {
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static SetAclAction valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static SetAclAction forNumber(int value) {
    switch (value) {
      case 0: return REPLACE;
      case 1: return MODIFY;
      case 2: return REMOVE;
      case 3: return REMOVE_ALL;
      case 4: return REMOVE_DEFAULT;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<SetAclAction>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      SetAclAction> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<SetAclAction>() {
          public SetAclAction findValueByNumber(int number) {
            return SetAclAction.forNumber(number);
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
    return alluxio.grpc.FileSystemMasterProto.getDescriptor().getEnumTypes().get(7);
  }

  private static final SetAclAction[] VALUES = values();

  public static SetAclAction valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private SetAclAction(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.file.SetAclAction)
}

