/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.table.under.hive;

import alluxio.grpc.BlockMetadata;
import alluxio.grpc.ColumnChunkMetaData;
import alluxio.grpc.ColumnPath;
import alluxio.grpc.FieldType;
import alluxio.grpc.FieldTypeId;
import alluxio.grpc.FileMetadata;
import alluxio.grpc.GroupType;
import alluxio.grpc.MessageType;
import alluxio.grpc.ParquetMetadata;
import alluxio.grpc.PrimitiveTypeName;
import alluxio.grpc.Repetition;
import alluxio.grpc.Schema;
import alluxio.grpc.Type;

import com.google.common.collect.ImmutableMap;
import io.grpc.Codec;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for hive types.
 */
public class HiveUtils {
  private HiveUtils() {} // prevent instantiation

  /**
   * @param hiveSchema the hive schema
   * @return the proto representation
   */
  public static Schema toProtoSchema(List<FieldSchema> hiveSchema) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.addAllCols(toProto(hiveSchema));
    return schemaBuilder.build();
  }

  /**
   * @param hiveSchema the hive schema
   * @return the proto representation
   */
  public static List<alluxio.grpc.FieldSchema> toProto(List<FieldSchema> hiveSchema) {
    List<alluxio.grpc.FieldSchema> list = new ArrayList<>();
    for (FieldSchema field : hiveSchema) {
      alluxio.grpc.FieldSchema aFieldSchema = alluxio.grpc.FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(Type.newBuilder().setType(toProto(field.getType())))
          .build();
      list.add(aFieldSchema);
    }
    return list;
  }

  private static FieldTypeId toProto(String hiveType) {
    switch (hiveType) {
      case "boolean": return FieldTypeId.BOOLEAN;
      case "tinyint": return FieldTypeId.INTEGER;
      case "smallint": return FieldTypeId.INTEGER;
      case "int": return FieldTypeId.INTEGER;
      case "integer": return FieldTypeId.INTEGER;
      case "bigint": return FieldTypeId.LONG;
      case "float": return FieldTypeId.FLOAT;
      case "double": return FieldTypeId.DOUBLE;
      case "decimal": return FieldTypeId.DECIMAL;
      case "numeric": return FieldTypeId.DECIMAL;
      case "date": return FieldTypeId.DATE;
      case "timestamp": return FieldTypeId.TIMESTAMP;
      case "string": return FieldTypeId.STRING;
      case "char": return FieldTypeId.STRING;
      case "varchar": return FieldTypeId.STRING;
      case "binary": return FieldTypeId.BINARY;
      default: // fall through
    }
    if (hiveType.startsWith("map<")) {
      return FieldTypeId.MAP;
    } else if (hiveType.startsWith("struct<")) {
      return FieldTypeId.STRUCT;
    } else if (hiveType.startsWith("decimal(")) {
      return FieldTypeId.DECIMAL;
    }
    throw new IllegalArgumentException("Unsupported hive type: " + hiveType);
  }

  private static Map<PrimitiveType.PrimitiveTypeName, PrimitiveTypeName> typeMap
    = new ImmutableMap.Builder<PrimitiveType.PrimitiveTypeName, PrimitiveTypeName>()
      .put(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveTypeName.PARQUETTYPE_BINARY)
      .put(PrimitiveType.PrimitiveTypeName.INT32, PrimitiveTypeName.PARQUETTYPE_INT32)
      .put(PrimitiveType.PrimitiveTypeName.INT64, PrimitiveTypeName.PARQUETTYPE_INT64)
      .put(PrimitiveType.PrimitiveTypeName.BOOLEAN, PrimitiveTypeName.PARQUETTYPE_BOOLEAN)
      .put(PrimitiveType.PrimitiveTypeName.INT96, PrimitiveTypeName.PARQUETTYPE_INT96)
      .put(PrimitiveType.PrimitiveTypeName.FLOAT, PrimitiveTypeName.PARQUETTYPE_FLOAT)
      .put(PrimitiveType.PrimitiveTypeName.DOUBLE, PrimitiveTypeName.PARQUETTYPE_DOUBLE)
      .put(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, PrimitiveTypeName.PARQUETTYPE_FIXED_LEN_BYTE_ARRAY)
      .build();

  private static Map<org.apache.parquet.schema.Type.Repetition, Repetition> repetitionMap
      = ImmutableMap.of(org.apache.parquet.schema.Type.Repetition.OPTIONAL, Repetition.OPTIONAL,
      org.apache.parquet.schema.Type.Repetition.REPEATED, Repetition.REPEATED,
      org.apache.parquet.schema.Type.Repetition.REQUIRED, Repetition.REQUIRED);

  private static Map<org.apache.parquet.column.Encoding,
      ColumnChunkMetaData.Encoding> encodingMap =
      new ImmutableMap.Builder<org.apache.parquet.column.Encoding,
      ColumnChunkMetaData.Encoding>()
      .put(Encoding.PLAIN, ColumnChunkMetaData.Encoding.PLAIN)
      .put(Encoding.RLE, ColumnChunkMetaData.Encoding.RLE)
      .put(Encoding.RLE_DICTIONARY, ColumnChunkMetaData.Encoding.RLE_DICTIONARY)
      .put(Encoding.DELTA_BINARY_PACKED, ColumnChunkMetaData.Encoding.DELTA_BINARY_PACKED)
      .put(Encoding.DELTA_BYTE_ARRAY, ColumnChunkMetaData.Encoding.DELTA_BYTE_ARRAY)
      .put(Encoding.DELTA_LENGTH_BYTE_ARRAY, ColumnChunkMetaData.Encoding.DELTA_LENGTH_BYTE_ARRAY)
      .put(Encoding.BIT_PACKED, ColumnChunkMetaData.Encoding.BIT_PACKED)
      .put(Encoding.PLAIN_DICTIONARY, ColumnChunkMetaData.Encoding.PLAIN_DICTIONARY)
      .build();

  private static Map<CompressionCodecName, ColumnChunkMetaData.CompressionCodecName> codecMap =
      new ImmutableMap.Builder<CompressionCodecName, ColumnChunkMetaData.CompressionCodecName>()
          .put(CompressionCodecName.BROTLI, ColumnChunkMetaData.CompressionCodecName.BROTLI)
          .put(CompressionCodecName.GZIP, ColumnChunkMetaData.CompressionCodecName.GZIP)
          .put(CompressionCodecName.LZ4, ColumnChunkMetaData.CompressionCodecName.LZ4)
          .put(CompressionCodecName.LZO, ColumnChunkMetaData.CompressionCodecName.LZO)
          .put(CompressionCodecName.SNAPPY, ColumnChunkMetaData.CompressionCodecName.SNAPPY)
          .put(CompressionCodecName.UNCOMPRESSED, ColumnChunkMetaData.CompressionCodecName.UNCOMPRESSED)
          .put(CompressionCodecName.ZSTD, ColumnChunkMetaData.CompressionCodecName.ZSTD)
          .build();

  private static FieldType toProto(org.apache.parquet.schema.Type type) {
    FieldType.Builder builder = FieldType.newBuilder();
    builder.setName(type.getName()).setRepetition(repetitionMap.get(type.getRepetition()));
    if (type.isPrimitive()) {
      builder.setTypeId(typeMap.get(type.asPrimitiveType().getPrimitiveTypeName()));
    } else {
      builder.setName(type.getName()).setGroup(GroupType.newBuilder().
          addAllFields(type.asGroupType().getFields().stream()
          .map(HiveUtils::toProto).collect(Collectors.toList())).build());
    }
    return builder.build();
  }
  private static MessageType toProto(org.apache.parquet.schema.MessageType type) {
    MessageType.Builder builder = MessageType.newBuilder();
    builder.setName(type.getName());
    builder.addAllType(type.getFields().stream().map(HiveUtils::toProto).collect(Collectors.toList()));
    return builder.build();
  }

  private static FileMetadata toProto(FileMetaData fileMetadata) {
    FileMetadata.Builder builder = FileMetadata.newBuilder();
    return builder.setCreatedBy(fileMetadata.getCreatedBy())
        .setSchema(toProto(fileMetadata.getSchema()))
        .putAllKeyValueMetadata(fileMetadata.getKeyValueMetaData())
        .build();
  }
  public static ParquetMetadata toProto(
      org.apache.parquet.hadoop.metadata.ParquetMetadata parquetMetadata) {
    ParquetMetadata.Builder builder = ParquetMetadata.newBuilder();
    builder.setFileMetadata(toProto(parquetMetadata.getFileMetaData()));
    builder.addAllBlockMetadata(parquetMetadata.getBlocks().stream().map(HiveUtils::toProto)
    .collect(Collectors.toList()));
    return builder.build();
  }

  private static BlockMetadata toProto(BlockMetaData blockMetaData) {
    BlockMetadata.Builder builder = BlockMetadata.newBuilder();
    builder.setPath(blockMetaData.getPath())
        .setRowCount(blockMetaData.getRowCount())
        .setTotalByteCount(blockMetaData.getTotalByteSize());
    builder.addAllColData(blockMetaData.getColumns().stream().map(HiveUtils::toProto).collect(Collectors.toList()));
    return builder.build();
  }

  private static ColumnPath toProto(org.apache.parquet.hadoop.metadata.ColumnPath columnPath) {
    return ColumnPath.newBuilder().addAllPathSegment(() -> columnPath.iterator()).build();
  }

  private static ColumnChunkMetaData toProto(
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData columnChunkMetaData) {
    alluxio.grpc.ColumnChunkMetaData.Builder builder = alluxio.grpc.ColumnChunkMetaData.newBuilder();
    builder.setPath(toProto(columnChunkMetaData.getPath()))
        .setFirstDataPage(columnChunkMetaData.getFirstDataPageOffset())
        .setPageOffset(columnChunkMetaData.getDictionaryPageOffset())
        .setTotalSize(columnChunkMetaData.getTotalSize())
        .setTotalUncompressedSize(columnChunkMetaData.getTotalUncompressedSize())
        .setValueCount(columnChunkMetaData.getValueCount());
    builder.addAllEncodings(columnChunkMetaData.getEncodings().stream()
        .map(x -> encodingMap.get(x)).collect(Collectors.toList()));
    builder.setType(typeMap.get(columnChunkMetaData.getType()));
    builder.setCodec(codecMap.get(columnChunkMetaData.getCodec()));
    return builder.build();
  }
}
