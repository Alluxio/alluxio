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

import alluxio.grpc.catalog.BinaryColumnStatsData;
import alluxio.grpc.catalog.BlockMetadata;
import alluxio.grpc.catalog.BooleanColumnStatsData;
import alluxio.grpc.catalog.ColumnChunkMetaData;
import alluxio.grpc.catalog.ColumnPath;
import alluxio.grpc.catalog.ColumnStatisticsData;
import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.Date;
import alluxio.grpc.catalog.DateColumnStatsData;
import alluxio.grpc.catalog.Decimal;
import alluxio.grpc.catalog.DecimalColumnStatsData;
import alluxio.grpc.catalog.DoubleColumnStatsData;
import alluxio.grpc.catalog.FieldType;
import alluxio.grpc.catalog.FieldTypeId;
import alluxio.grpc.catalog.FileMetadata;
import alluxio.grpc.catalog.GroupType;
import alluxio.grpc.catalog.HiveBucketProperty;
import alluxio.grpc.catalog.LongColumnStatsData;
import alluxio.grpc.catalog.MessageType;
import alluxio.grpc.catalog.ParquetMetadata;
import alluxio.grpc.catalog.PrimitiveTypeName;
import alluxio.grpc.catalog.Repetition;
import alluxio.grpc.catalog.Schema;
import alluxio.grpc.catalog.SortingColumn;
import alluxio.grpc.catalog.Storage;
import alluxio.grpc.catalog.StorageFormat;
import alluxio.grpc.catalog.StringColumnStatsData;
import alluxio.table.under.hive.util.PathTranslator;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  public static List<alluxio.grpc.catalog.FieldSchema> toProto(List<FieldSchema> hiveSchema) {
    List<alluxio.grpc.catalog.FieldSchema> list = new ArrayList<>();
    for (FieldSchema field : hiveSchema) {
      alluxio.grpc.catalog.FieldSchema aFieldSchema = alluxio.grpc.catalog.FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(field.getType()) // does not support complex types now
          .setComment(field.getComment() != null ? field.getComment() : "")
          .build();
      list.add(aFieldSchema);
    }
    return list;
  }

  /**
   * Convert from a StorageDescriptor to a Storage object.
   *
   * @param sd storage descriptor
   * @param translator path translator
   * @return storage proto object
   */
  public static Storage toProto(StorageDescriptor sd, PathTranslator translator)
      throws IOException {
    if (sd == null) {
      return Storage.getDefaultInstance();
    }
    String serDe = sd.getSerdeInfo() == null ? ""
        : sd.getSerdeInfo().getSerializationLib();
    StorageFormat format = StorageFormat.newBuilder()
        .setInputFormat(sd.getInputFormat())
        .setOutputFormat(sd.getOutputFormat())
        .setSerDe(serDe).build(); // Check SerDe info
    Storage.Builder storageBuilder = Storage.newBuilder();
    List<Order> orderList = sd.getSortCols();
    List<SortingColumn> sortingColumns;
    if (orderList == null) {
      sortingColumns = Collections.emptyList();
    } else {
      sortingColumns = orderList.stream().map(
          order -> SortingColumn.newBuilder().setColumnName(order.getCol())
              .setOrder(order.getOrder() == 1 ? SortingColumn.SortingOrder.ASCENDING
                  : SortingColumn.SortingOrder.DESCENDING).build())
          .collect(Collectors.toList());
    }
    return storageBuilder.setStorageFormat(format)
        .setLocation(translator.toAlluxioPath(sd.getLocation()))
        .setBucketProperty(HiveBucketProperty.newBuilder().setBucketCount(sd.getNumBuckets())
            .addAllBucketedBy(sd.getBucketCols()).addAllSortedBy(sortingColumns).build())
        .setSkewed(sd.getSkewedInfo() != null && (sd.getSkewedInfo().getSkewedColNames()) != null
            && !sd.getSkewedInfo().getSkewedColNames().isEmpty())
        .putAllSerdeParameters(sd.getParameters()).build();
  }

  /**
   * Convert ColumnStatisticsObj to proto definition.
   *
   * @param colStats column statistics
   * @return the proto form
   */
  public static ColumnStatisticsInfo toProto(ColumnStatisticsObj colStats) {
    ColumnStatisticsInfo.Builder builder = ColumnStatisticsInfo.newBuilder();
    builder.setColName(colStats.getColName()).setColType(colStats.getColType());
    org.apache.hadoop.hive.metastore.api.ColumnStatisticsData statsData = colStats.getStatsData();
    if (statsData.isSetBooleanStats()) {
      org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData data =
          statsData.getBooleanStats();
      if (data != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setBooleanStats(BooleanColumnStatsData.newBuilder()
                .setNumTrues(data.getNumTrues()).setNumFalses(data.getNumFalses())
                .setNumNulls(data.getNumNulls()).setBitVectors(data.getBitVectors())
                .build()).build());
      }
    }
    if (statsData.isSetDoubleStats()) {
      org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData doubleStats =
          statsData.getDoubleStats();
      if (doubleStats != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setDoubleStats(DoubleColumnStatsData.newBuilder()
                .setNumDVs(doubleStats.getNumDVs()).setHighValue(doubleStats.getHighValue())
                .setLowValue(doubleStats.getLowValue()).setNumNulls(doubleStats.getNumNulls())
                .setBitVectors(doubleStats.getBitVectors()).build()).build());
      }
    }
    if (statsData.isSetLongStats()) {
      org.apache.hadoop.hive.metastore.api.LongColumnStatsData longData =
          statsData.getLongStats();
      if (longData != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setLongStats(LongColumnStatsData.newBuilder()
                .setNumDVs(longData.getNumDVs()).setHighValue(longData.getHighValue())
                .setLowValue(longData.getLowValue())
                .setNumNulls(longData.getNumNulls()).setBitVectors(longData.getBitVectors())
                .build()).build());
      }
    }
    if (statsData.isSetStringStats()) {
      org.apache.hadoop.hive.metastore.api.StringColumnStatsData stringData =
          statsData.getStringStats();
      if (stringData != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setStringStats(StringColumnStatsData.newBuilder()
                .setNumDVs(stringData.getNumDVs()).setAvgColLen(stringData.getAvgColLen())
                .setMaxColLen(stringData.getMaxColLen())
                .setNumNulls(stringData.getNumNulls()).setBitVectors(stringData.getBitVectors())
                .build()).build());
      }
    }
    if (statsData.isSetStringStats()) {
      org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData data =
          statsData.getBinaryStats();
      if (data != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setBinaryStats(BinaryColumnStatsData.newBuilder()
                .setMaxColLen(data.getMaxColLen()).setAvgColLen(data.getAvgColLen())
                .setNumNulls(data.getNumNulls()).setBitVectors(data.getBitVectors())
                .build()).build());
      }
    }
    if (statsData.isSetDateStats()) {
      org.apache.hadoop.hive.metastore.api.DateColumnStatsData data =
          statsData.getDateStats();
      if (data != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setDateStats(DateColumnStatsData.newBuilder()
                .setHighValue(toProto(data.getHighValue()))
                .setLowValue(toProto(data.getLowValue()))
                .setNumNulls(data.getNumNulls())
                .setNumDVs(data.getNumDVs())
                .setBitVectors(data.getBitVectors())
                .build()).build());
      }
    }

    if (statsData.isSetDecimalStats()) {
      org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData data =
          statsData.getDecimalStats();
      if (data != null) {
        builder.setData(ColumnStatisticsData.newBuilder()
            .setDecimalStats(DecimalColumnStatsData.newBuilder()
                .setHighValue(toProto(data.getHighValue()))
                .setLowValue(toProto(data.getLowValue()))
                .setNumNulls(data.getNumNulls())
                .setNumDVs(data.getNumDVs())
                .setBitVectors(data.getBitVectors())
                .build()).build());
      }
    }
    return builder.build();
  }

  private static Date toProto(org.apache.hadoop.hive.metastore.api.Date date) {
    return Date.newBuilder().setDaysSinceEpoch(date.getDaysSinceEpoch()).build();
  }

  private static Decimal toProto(org.apache.hadoop.hive.metastore.api.Decimal decimal) {
    return Decimal.newBuilder().setScale(decimal.getScale())
        .setUnscaled(ByteString.copyFrom(decimal.getUnscaled())).build();
  }

  private static FieldTypeId toProto(String hiveType) {
    switch (hiveType) {
      case "boolean": return FieldTypeId.BOOLEAN;
      case "tinyint": return FieldTypeId.BYTE;
      case "smallint": return FieldTypeId.SHORT;
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

  private static final BiMap<PrimitiveType.PrimitiveTypeName, PrimitiveTypeName> TYPEMAP
      = new ImmutableBiMap.Builder<PrimitiveType.PrimitiveTypeName, PrimitiveTypeName>()
      .put(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveTypeName.PARQUETTYPE_BINARY)
      .put(PrimitiveType.PrimitiveTypeName.INT32, PrimitiveTypeName.PARQUETTYPE_INT32)
      .put(PrimitiveType.PrimitiveTypeName.INT64, PrimitiveTypeName.PARQUETTYPE_INT64)
      .put(PrimitiveType.PrimitiveTypeName.BOOLEAN, PrimitiveTypeName.PARQUETTYPE_BOOLEAN)
      .put(PrimitiveType.PrimitiveTypeName.INT96, PrimitiveTypeName.PARQUETTYPE_INT96)
      .put(PrimitiveType.PrimitiveTypeName.FLOAT, PrimitiveTypeName.PARQUETTYPE_FLOAT)
      .put(PrimitiveType.PrimitiveTypeName.DOUBLE, PrimitiveTypeName.PARQUETTYPE_DOUBLE)
      .put(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
          PrimitiveTypeName.PARQUETTYPE_FIXED_LEN_BYTE_ARRAY)
      .build();

  private static final BiMap<org.apache.parquet.schema.Type.Repetition, Repetition> REPETITIONMAP
      = ImmutableBiMap.of(org.apache.parquet.schema.Type.Repetition.OPTIONAL, Repetition.OPTIONAL,
      org.apache.parquet.schema.Type.Repetition.REPEATED, Repetition.REPEATED,
      org.apache.parquet.schema.Type.Repetition.REQUIRED, Repetition.REQUIRED);

  private static final BiMap<org.apache.parquet.column.Encoding,
      ColumnChunkMetaData.Encoding> ENCODINGMAP =
      new ImmutableBiMap.Builder<org.apache.parquet.column.Encoding,
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

  private static final BiMap<CompressionCodecName,
      ColumnChunkMetaData.CompressionCodecName> CODECMAP =
      new ImmutableBiMap.Builder<CompressionCodecName, ColumnChunkMetaData.CompressionCodecName>()
          .put(CompressionCodecName.BROTLI, ColumnChunkMetaData.CompressionCodecName.BROTLI)
          .put(CompressionCodecName.GZIP, ColumnChunkMetaData.CompressionCodecName.GZIP)
          .put(CompressionCodecName.LZ4, ColumnChunkMetaData.CompressionCodecName.LZ4)
          .put(CompressionCodecName.LZO, ColumnChunkMetaData.CompressionCodecName.LZO)
          .put(CompressionCodecName.SNAPPY, ColumnChunkMetaData.CompressionCodecName.SNAPPY)
          .put(CompressionCodecName.UNCOMPRESSED,
              ColumnChunkMetaData.CompressionCodecName.UNCOMPRESSED)
          .put(CompressionCodecName.ZSTD, ColumnChunkMetaData.CompressionCodecName.ZSTD)
          .build();

  private static FieldType toProto(org.apache.parquet.schema.Type type) {
    FieldType.Builder builder = FieldType.newBuilder();
    builder.setName(type.getName()).setRepetition(REPETITIONMAP.get(type.getRepetition()));
    if (type.isPrimitive()) {
      builder.setTypeId(TYPEMAP.get(type.asPrimitiveType().getPrimitiveTypeName()));
    } else {
      builder.setName(type.getName()).setGroup(GroupType.newBuilder()
          .addAllFields(type.asGroupType().getFields().stream()
          .map(HiveUtils::toProto).collect(Collectors.toList())).build());
    }
    return builder.build();
  }

  private static MessageType toProto(org.apache.parquet.schema.MessageType type) {
    MessageType.Builder builder = MessageType.newBuilder();
    builder.setName(type.getName());
    builder.addAllType(type.getFields().stream().map(
        HiveUtils::toProto).collect(Collectors.toList()));
    return builder.build();
  }

  private static FileMetadata toProto(FileMetaData fileMetadata) {
    FileMetadata.Builder builder = FileMetadata.newBuilder();
    return builder.setCreatedBy(fileMetadata.getCreatedBy())
        .setSchema(toProto(fileMetadata.getSchema()))
        .putAllKeyValueMetadata(fileMetadata.getKeyValueMetaData())
        .build();
  }

  /**
   * Convert ParquetMetadata to proto represetation.
   *
   * @param parquetMetadata parquet metadata
   * @return proto representation of the parquet metadata
   */
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
    if (blockMetaData.getPath() != null) {
      builder.setPath(blockMetaData.getPath());
    }
    builder.setRowCount(blockMetaData.getRowCount())
        .setTotalByteCount(blockMetaData.getTotalByteSize());
    builder.addAllColData(blockMetaData.getColumns().stream().map(
        HiveUtils::toProto).collect(Collectors.toList()));
    return builder.build();
  }

  private static ColumnPath toProto(org.apache.parquet.hadoop.metadata.ColumnPath columnPath) {
    return ColumnPath.newBuilder().addAllPathSegment(() -> columnPath.iterator()).build();
  }

  private static ColumnChunkMetaData toProto(
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData columnChunkMetaData) {
    alluxio.grpc.catalog.ColumnChunkMetaData.Builder builder =
        alluxio.grpc.catalog.ColumnChunkMetaData.newBuilder();
    builder.setPath(toProto(columnChunkMetaData.getPath()))
        .setFirstDataPage(columnChunkMetaData.getFirstDataPageOffset())
        .setPageOffset(columnChunkMetaData.getDictionaryPageOffset())
        .setTotalSize(columnChunkMetaData.getTotalSize())
        .setTotalUncompressedSize(columnChunkMetaData.getTotalUncompressedSize())
        .setValueCount(columnChunkMetaData.getValueCount());
    builder.addAllEncodings(columnChunkMetaData.getEncodings().stream()
        .map(x -> ENCODINGMAP.get(x)).collect(Collectors.toList()));
    builder.setType(TYPEMAP.get(columnChunkMetaData.getType()));
    builder.setCodec(CODECMAP.get(columnChunkMetaData.getCodec()));
    return builder.build();
  }

  /**
   * Convert from the proto represetation to ParquetMetadata.
   *
   * @param parquetMetadata parquet metadata
   * @return proto representation of the parquet metadata
   */
  public static org.apache.parquet.hadoop.metadata.ParquetMetadata fromProto(
      ParquetMetadata parquetMetadata) {
    return new org.apache.parquet.hadoop.metadata.ParquetMetadata(
        fromProto(parquetMetadata.getFileMetadata()),
        parquetMetadata.getBlockMetadataList().stream().map(HiveUtils::fromProto)
            .collect(Collectors.toList()));
  }

  private static BlockMetaData fromProto(BlockMetadata blockMetadata) {
    BlockMetaData metadata = new BlockMetaData();
    for (ColumnChunkMetaData chunkMetaData : blockMetadata.getColDataList()) {
      metadata.addColumn(fromProto(chunkMetaData));
    }
    metadata.setPath(blockMetadata.getPath());
    metadata.setRowCount(blockMetadata.getRowCount());
    metadata.setTotalByteSize(blockMetadata.getTotalByteCount());
    return metadata;
  }

  private static org.apache.parquet.hadoop.metadata.ColumnChunkMetaData fromProto(
      ColumnChunkMetaData chunkMetaData) {
    // use a non-deprecated constructor, add stats to the proto
    return org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
        org.apache.parquet.hadoop.metadata.ColumnPath.get(
            chunkMetaData.getPath().getPathSegmentList().toArray(new String[0])),
        TYPEMAP.inverse().get(chunkMetaData.getType()),
        CODECMAP.inverse().get(chunkMetaData.getCodec()),
        null,
        chunkMetaData.getEncodingsList().stream().map(
            (x) -> ENCODINGMAP.inverse().get(x)).collect(Collectors.toSet()),
        null,
        chunkMetaData.getFirstDataPage(),
        chunkMetaData.getPageOffset(),
        chunkMetaData.getValueCount(),
        chunkMetaData.getTotalSize(),
        chunkMetaData.getTotalUncompressedSize()
    );
  }

  private static FileMetaData fromProto(FileMetadata parquetMetadata) {
    return new FileMetaData(fromProto(parquetMetadata.getSchema()),
        parquetMetadata.getKeyValueMetadataMap(),
        parquetMetadata.getCreatedBy());
  }

  private static org.apache.parquet.schema.MessageType fromProto(MessageType schema) {
    return new org.apache.parquet.schema.MessageType(schema.getName(),
        schema.getTypeList().stream().map(HiveUtils::fromProto).collect(Collectors.toList()));
  }

  private static org.apache.parquet.schema.Type fromProto(FieldType fieldType) {
    if (fieldType.hasTypeId()) {
      //primitive type
      return new org.apache.parquet.schema.PrimitiveType(
          REPETITIONMAP.inverse().get(fieldType.getRepetition()),
          TYPEMAP.inverse().get(fieldType.getTypeId()),
          fieldType.getName());
    } else {
      return new org.apache.parquet.schema.GroupType(
          REPETITIONMAP.inverse().get(fieldType.getRepetition()),
          fieldType.getName(),
          fieldType.getGroup().getFieldsList().stream().map(HiveUtils::fromProto)
              .collect(Collectors.toList()));
    }
  }
}
