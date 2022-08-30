// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.meta.MetaCommand}
 */
public enum MetaCommand
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>MetaCommand_Unknown = 0;</code>
   */
  MetaCommand_Unknown(0),
  /**
   * <code>MetaCommand_Nothing = 1;</code>
   */
  MetaCommand_Nothing(1),
  /**
   * <pre>
   * Ask the standby master to re-register.
   * </pre>
   *
   * <code>MetaCommand_Register = 2;</code>
   */
  MetaCommand_Register(2),
  ;

  /**
   * <code>MetaCommand_Unknown = 0;</code>
   */
  public static final int MetaCommand_Unknown_VALUE = 0;
  /**
   * <code>MetaCommand_Nothing = 1;</code>
   */
  public static final int MetaCommand_Nothing_VALUE = 1;
  /**
   * <pre>
   * Ask the standby master to re-register.
   * </pre>
   *
   * <code>MetaCommand_Register = 2;</code>
   */
  public static final int MetaCommand_Register_VALUE = 2;


  public final int getNumber() {
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static MetaCommand valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static MetaCommand forNumber(int value) {
    switch (value) {
      case 0: return MetaCommand_Unknown;
      case 1: return MetaCommand_Nothing;
      case 2: return MetaCommand_Register;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<MetaCommand>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      MetaCommand> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<MetaCommand>() {
          public MetaCommand findValueByNumber(int number) {
            return MetaCommand.forNumber(number);
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
    return alluxio.grpc.MetaMasterProto.getDescriptor().getEnumTypes().get(4);
  }

  private static final MetaCommand[] VALUES = values();

  public static MetaCommand valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private MetaCommand(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.meta.MetaCommand)
}

