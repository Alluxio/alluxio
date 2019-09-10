// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.Encoding}
 */
public enum Encoding
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>PLAIN = 0;</code>
   */
  PLAIN(0),
  /**
   * <code>RLE = 1;</code>
   */
  RLE(1),
  /**
   * <code>BIT_PACKED = 2;</code>
   */
  BIT_PACKED(2),
  /**
   * <code>PLAIN_DICTIONARY = 3;</code>
   */
  PLAIN_DICTIONARY(3),
  /**
   * <code>DELTA_BINARY_PACKED = 4;</code>
   */
  DELTA_BINARY_PACKED(4),
  /**
   * <code>DELTA_LENGTH_BYTE_ARRAY = 5;</code>
   */
  DELTA_LENGTH_BYTE_ARRAY(5),
  /**
   * <code>DELTA_BYTE_ARRAY = 6;</code>
   */
  DELTA_BYTE_ARRAY(6),
  /**
   * <code>RLE_DICTIONARY = 7;</code>
   */
  RLE_DICTIONARY(7),
  ;

  /**
   * <code>PLAIN = 0;</code>
   */
  public static final int PLAIN_VALUE = 0;
  /**
   * <code>RLE = 1;</code>
   */
  public static final int RLE_VALUE = 1;
  /**
   * <code>BIT_PACKED = 2;</code>
   */
  public static final int BIT_PACKED_VALUE = 2;
  /**
   * <code>PLAIN_DICTIONARY = 3;</code>
   */
  public static final int PLAIN_DICTIONARY_VALUE = 3;
  /**
   * <code>DELTA_BINARY_PACKED = 4;</code>
   */
  public static final int DELTA_BINARY_PACKED_VALUE = 4;
  /**
   * <code>DELTA_LENGTH_BYTE_ARRAY = 5;</code>
   */
  public static final int DELTA_LENGTH_BYTE_ARRAY_VALUE = 5;
  /**
   * <code>DELTA_BYTE_ARRAY = 6;</code>
   */
  public static final int DELTA_BYTE_ARRAY_VALUE = 6;
  /**
   * <code>RLE_DICTIONARY = 7;</code>
   */
  public static final int RLE_DICTIONARY_VALUE = 7;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static Encoding valueOf(int value) {
    return forNumber(value);
  }

  public static Encoding forNumber(int value) {
    switch (value) {
      case 0: return PLAIN;
      case 1: return RLE;
      case 2: return BIT_PACKED;
      case 3: return PLAIN_DICTIONARY;
      case 4: return DELTA_BINARY_PACKED;
      case 5: return DELTA_LENGTH_BYTE_ARRAY;
      case 6: return DELTA_BYTE_ARRAY;
      case 7: return RLE_DICTIONARY;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<Encoding>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      Encoding> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<Encoding>() {
          public Encoding findValueByNumber(int number) {
            return Encoding.forNumber(number);
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
    return alluxio.grpc.CatalogMasterProto.getDescriptor().getEnumTypes().get(4);
  }

  private static final Encoding[] VALUES = values();

  public static Encoding valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private Encoding(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.Encoding)
}

