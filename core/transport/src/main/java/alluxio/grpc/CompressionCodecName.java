// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog_master.proto

package alluxio.grpc;

/**
 * Protobuf enum {@code alluxio.grpc.CompressionCodecName}
 */
public enum CompressionCodecName
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>UNCOMPRESSED = 0;</code>
   */
  UNCOMPRESSED(0),
  /**
   * <code>SNAPPY = 1;</code>
   */
  SNAPPY(1),
  /**
   * <code>GZIP = 2;</code>
   */
  GZIP(2),
  /**
   * <code>LZO = 3;</code>
   */
  LZO(3),
  /**
   * <code>BROTLI = 4;</code>
   */
  BROTLI(4),
  /**
   * <code>LZ4 = 5;</code>
   */
  LZ4(5),
  /**
   * <code>ZSTD = 6;</code>
   */
  ZSTD(6),
  ;

  /**
   * <code>UNCOMPRESSED = 0;</code>
   */
  public static final int UNCOMPRESSED_VALUE = 0;
  /**
   * <code>SNAPPY = 1;</code>
   */
  public static final int SNAPPY_VALUE = 1;
  /**
   * <code>GZIP = 2;</code>
   */
  public static final int GZIP_VALUE = 2;
  /**
   * <code>LZO = 3;</code>
   */
  public static final int LZO_VALUE = 3;
  /**
   * <code>BROTLI = 4;</code>
   */
  public static final int BROTLI_VALUE = 4;
  /**
   * <code>LZ4 = 5;</code>
   */
  public static final int LZ4_VALUE = 5;
  /**
   * <code>ZSTD = 6;</code>
   */
  public static final int ZSTD_VALUE = 6;


  public final int getNumber() {
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static CompressionCodecName valueOf(int value) {
    return forNumber(value);
  }

  public static CompressionCodecName forNumber(int value) {
    switch (value) {
      case 0: return UNCOMPRESSED;
      case 1: return SNAPPY;
      case 2: return GZIP;
      case 3: return LZO;
      case 4: return BROTLI;
      case 5: return LZ4;
      case 6: return ZSTD;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<CompressionCodecName>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      CompressionCodecName> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<CompressionCodecName>() {
          public CompressionCodecName findValueByNumber(int number) {
            return CompressionCodecName.forNumber(number);
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
    return alluxio.grpc.CatalogMasterProto.getDescriptor().getEnumTypes().get(3);
  }

  private static final CompressionCodecName[] VALUES = values();

  public static CompressionCodecName valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private CompressionCodecName(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:alluxio.grpc.CompressionCodecName)
}

