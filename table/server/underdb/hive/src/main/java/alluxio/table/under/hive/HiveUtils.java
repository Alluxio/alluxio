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

import alluxio.grpc.table.BinaryColumnStatsData;
import alluxio.grpc.table.BooleanColumnStatsData;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Date;
import alluxio.grpc.table.DateColumnStatsData;
import alluxio.grpc.table.Decimal;
import alluxio.grpc.table.DecimalColumnStatsData;
import alluxio.grpc.table.DoubleColumnStatsData;
import alluxio.grpc.table.LongColumnStatsData;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.StringColumnStatsData;
import alluxio.grpc.table.layout.hive.HiveBucketProperty;
import alluxio.grpc.table.layout.hive.SortingColumn;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.grpc.table.layout.hive.StorageFormat;
import alluxio.table.common.udb.PathTranslator;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  public static List<alluxio.grpc.table.FieldSchema> toProto(List<FieldSchema> hiveSchema) {
    List<alluxio.grpc.table.FieldSchema> list = new ArrayList<>();
    for (FieldSchema field : hiveSchema) {
      alluxio.grpc.table.FieldSchema.Builder builder = alluxio.grpc.table.FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(field.getType()); // does not support complex types now
      if (field.isSetComment()) {
        builder.setComment(field.getComment());
      }
      list.add(builder.build());
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
    String serDe = sd.getSerdeInfo() == null ? null
        : sd.getSerdeInfo().getSerializationLib();
    Map<String, String> serdeLibMap = sd.getSerdeInfo() == null ? null
        : sd.getSerdeInfo().getParameters();
    StorageFormat.Builder formatBuilder = StorageFormat.newBuilder()
        .setInputFormat(sd.getInputFormat())
        .setOutputFormat(sd.getOutputFormat());

    if (serdeLibMap != null) {
      formatBuilder.putAllSerdelibParameters(serdeLibMap);
    }
    if (serDe != null) {
      formatBuilder.setSerde(serDe); // Check SerDe info
    }
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

    return storageBuilder.setStorageFormat(formatBuilder.build())
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
    if (colStats.isSetStatsData()) {
      org.apache.hadoop.hive.metastore.api.ColumnStatisticsData statsData = colStats.getStatsData();
      if (statsData.isSetBooleanStats()) {
        org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData data =
            statsData.getBooleanStats();
        if (data != null) {
          BooleanColumnStatsData.Builder booleanColumnBuilder = BooleanColumnStatsData.newBuilder()
              .setNumTrues(data.getNumTrues()).setNumFalses(data.getNumFalses())
              .setNumNulls(data.getNumNulls());
          if (data.isSetBitVectors()) {
            booleanColumnBuilder.setBitVectors(data.getBitVectors());
          }
          builder.setData(ColumnStatisticsData.newBuilder()
              .setBooleanStats(booleanColumnBuilder.build()).build());
        }
      }
      if (statsData.isSetDoubleStats()) {
        org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData doubleStats =
            statsData.getDoubleStats();
        if (doubleStats != null) {
          builder.setData(
              ColumnStatisticsData.newBuilder().setDoubleStats(toProto(doubleStats)).build());
        }
      }
      if (statsData.isSetLongStats()) {
        org.apache.hadoop.hive.metastore.api.LongColumnStatsData longData =
            statsData.getLongStats();

        if (longData != null) {
          builder.setData(ColumnStatisticsData.newBuilder()
              .setLongStats(toProto(longData)).build());
        }
      }
      if (statsData.isSetStringStats()) {
        org.apache.hadoop.hive.metastore.api.StringColumnStatsData stringData =
            statsData.getStringStats();
        if (stringData != null) {
          StringColumnStatsData.Builder stringColumnBuilder = StringColumnStatsData.newBuilder()
              .setNumDistincts(stringData.getNumDVs()).setAvgColLen(stringData.getAvgColLen())
              .setMaxColLen(stringData.getMaxColLen())
              .setNumNulls(stringData.getNumNulls());
          if (stringData.isSetBitVectors()) {
            stringColumnBuilder.setBitVectors(stringData.getBitVectors());
          }
          builder.setData(ColumnStatisticsData.newBuilder()
              .setStringStats(stringColumnBuilder.build()).build());
        }
      }
      if (statsData.isSetBinaryStats()) {
        org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData data =
            statsData.getBinaryStats();
        if (data != null) {
          BinaryColumnStatsData.Builder binaryColumnBuilder = BinaryColumnStatsData.newBuilder()
              .setMaxColLen(data.getMaxColLen()).setAvgColLen(data.getAvgColLen())
              .setNumNulls(data.getNumNulls());
          if (data.isSetBitVectors()) {
            binaryColumnBuilder.setBitVectors(data.getBitVectors());
          }
          builder.setData(ColumnStatisticsData.newBuilder()
              .setBinaryStats(binaryColumnBuilder.build()).build());
        }
      }
      if (statsData.isSetDateStats()) {
        org.apache.hadoop.hive.metastore.api.DateColumnStatsData data =
            statsData.getDateStats();
        if (data != null) {
          builder.setData(ColumnStatisticsData.newBuilder().setDateStats(toProto(data)).build());
        }
      }
      if (statsData.isSetDecimalStats()) {
        org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData data =
            statsData.getDecimalStats();
        if (data != null) {
          builder.setData(ColumnStatisticsData.newBuilder().setDecimalStats(toProto(data)).build());
        }
      }
    }
    return builder.build();
  }

  private static DateColumnStatsData toProto(
      org.apache.hadoop.hive.metastore.api.DateColumnStatsData dateData) {
    DateColumnStatsData.Builder builder = DateColumnStatsData.newBuilder();
    if (dateData.isSetBitVectors()) {
      builder.setBitVectors(dateData.getBitVectors());
    }
    builder.setNumNulls(dateData.getNumNulls());
    builder.setNumDistincts(dateData.getNumDVs());
    if (dateData.isSetHighValue()) {
      builder.setHighValue(
          Date.newBuilder().setDaysSinceEpoch(dateData.getHighValue().getDaysSinceEpoch()).build());
    }
    if (dateData.isSetLowValue()) {
      builder.setLowValue(
          Date.newBuilder().setDaysSinceEpoch(dateData.getLowValue().getDaysSinceEpoch()).build());
    }
    return builder.build();
  }

  private static DecimalColumnStatsData toProto(
      org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData decimalData) {
    DecimalColumnStatsData.Builder builder = DecimalColumnStatsData.newBuilder();
    if (decimalData.isSetBitVectors()) {
      builder.setBitVectors(decimalData.getBitVectors());
    }
    builder.setNumNulls(decimalData.getNumNulls());
    builder.setNumDistincts(decimalData.getNumDVs());
    if (decimalData.isSetHighValue() && decimalData.getHighValue().isSetUnscaled()) {
      builder.setHighValue(Decimal.newBuilder().setScale(decimalData.getHighValue().getScale())
          .setUnscaled(ByteString.copyFrom(decimalData.getHighValue().getUnscaled())).build());
    }
    if (decimalData.isSetLowValue() && decimalData.getLowValue().isSetUnscaled()) {
      builder.setLowValue(Decimal.newBuilder().setScale(decimalData.getLowValue().getScale())
          .setUnscaled(ByteString.copyFrom(decimalData.getLowValue().getUnscaled())).build());
    }
    return builder.build();
  }

  private static DoubleColumnStatsData toProto(
      org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData doubleData) {
    DoubleColumnStatsData.Builder builder = DoubleColumnStatsData.newBuilder();
    if (doubleData.isSetBitVectors()) {
      builder.setBitVectors(doubleData.getBitVectors());
    }
    builder.setNumNulls(doubleData.getNumNulls());
    builder.setNumDistincts(doubleData.getNumDVs());
    if (doubleData.isSetHighValue()) {
      builder.setHighValue(doubleData.getHighValue());
    }
    if (doubleData.isSetLowValue()) {
      builder.setLowValue(doubleData.getLowValue());
    }
    return builder.build();
  }

  private static LongColumnStatsData toProto(
      org.apache.hadoop.hive.metastore.api.LongColumnStatsData longData) {
    LongColumnStatsData.Builder builder = LongColumnStatsData.newBuilder();
    if (longData.isSetBitVectors()) {
      builder.setBitVectors(longData.getBitVectors());
    }
    builder.setNumNulls(longData.getNumNulls());
    builder.setNumDistincts(longData.getNumDVs());
    if (longData.isSetHighValue()) {
      builder.setHighValue(longData.getHighValue());
    }
    if (longData.isSetLowValue()) {
      builder.setLowValue(longData.getLowValue());
    }
    return builder.build();
  }
}
