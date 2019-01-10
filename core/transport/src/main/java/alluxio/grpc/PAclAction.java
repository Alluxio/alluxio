// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.file.PAclAction}
 */
public enum PAclAction
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>Read = 0;</code>
   */
  Read(0),
  /**
   * <code>Write = 1;</code>
   */
  Write(1),
  /**
   * <code>Execute = 2;</code>
   */
  Execute(2),
  ;

  /**
   * <code>Read = 0;</code>
   */
  public static final int Read_VALUE = 0;
  /**
   * <code>Write = 1;</code>
   */
  public static final int Write_VALUE = 1;
  /**
   * <code>Execute = 2;</code>
   */
  public static final int Execute_VALUE = 2;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static PAclAction valueOf(int value) {
    return forNumber(value);
  }

  public static PAclAction forNumber(int value) {
    switch (value) {
      case 0: return Read;
      case 1: return Write;
      case 2: return Execute;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<PAclAction>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      PAclAction> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<PAclAction>() {
          public PAclAction findValueByNumber(int number) {
            return PAclAction.forNumber(number);
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
    return alluxio.grpc.FileSystemMasterProto.getDescriptor().getEnumTypes().get(5);
  }

  private static final PAclAction[] VALUES = values();

  public static PAclAction valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private PAclAction(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.file.PAclAction)
}

