// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog/catalog_master.proto

package alluxio.grpc.catalog;

/**
 * <pre>
 * Parquet metadata related types
 * </pre>
 *
 * Protobuf enum {@code alluxio.grpc.catalog.Repetition}
 */
public enum Repetition
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>REPEATED = 1;</code>
   */
  REPEATED(1),
  /**
   * <code>OPTIONAL = 2;</code>
   */
  OPTIONAL(2),
  /**
   * <code>REQUIRED = 3;</code>
   */
  REQUIRED(3),
  ;

  /**
   * <code>REPEATED = 1;</code>
   */
  public static final int REPEATED_VALUE = 1;
  /**
   * <code>OPTIONAL = 2;</code>
   */
  public static final int OPTIONAL_VALUE = 2;
  /**
   * <code>REQUIRED = 3;</code>
   */
  public static final int REQUIRED_VALUE = 3;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static Repetition valueOf(int value) {
    return forNumber(value);
  }

  public static Repetition forNumber(int value) {
    switch (value) {
      case 1: return REPEATED;
      case 2: return OPTIONAL;
      case 3: return REQUIRED;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<Repetition>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      Repetition> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<Repetition>() {
          public Repetition findValueByNumber(int number) {
            return Repetition.forNumber(number);
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
    return alluxio.grpc.catalog.CatalogMasterProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final Repetition[] VALUES = values();

  public static Repetition valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private Repetition(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.catalog.Repetition)
}

