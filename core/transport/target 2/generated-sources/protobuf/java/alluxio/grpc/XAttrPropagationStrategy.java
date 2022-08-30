// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * <pre>
 * XAttrPropagationStrategy controls the behaviour for assigning xAttr
 * on Inodes within nested directories
 * - NEW_PATHS: Assign xAttr for any missing nodes along the filepath
 * - LEAF_NODE: Only assign xAttr on the leaf node of the filepath
 * </pre>
 *
 * Protobuf enum {@code alluxio.grpc.file.XAttrPropagationStrategy}
 */
public enum XAttrPropagationStrategy
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>NEW_PATHS = 1;</code>
   */
  NEW_PATHS(1),
  /**
   * <code>LEAF_NODE = 2;</code>
   */
  LEAF_NODE(2),
  ;

  /**
   * <code>NEW_PATHS = 1;</code>
   */
  public static final int NEW_PATHS_VALUE = 1;
  /**
   * <code>LEAF_NODE = 2;</code>
   */
  public static final int LEAF_NODE_VALUE = 2;


  public final int getNumber() {
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static XAttrPropagationStrategy valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static XAttrPropagationStrategy forNumber(int value) {
    switch (value) {
      case 1: return NEW_PATHS;
      case 2: return LEAF_NODE;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<XAttrPropagationStrategy>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      XAttrPropagationStrategy> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<XAttrPropagationStrategy>() {
          public XAttrPropagationStrategy findValueByNumber(int number) {
            return XAttrPropagationStrategy.forNumber(number);
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
    return alluxio.grpc.FileSystemMasterProto.getDescriptor().getEnumTypes().get(3);
  }

  private static final XAttrPropagationStrategy[] VALUES = values();

  public static XAttrPropagationStrategy valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private XAttrPropagationStrategy(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.file.XAttrPropagationStrategy)
}

