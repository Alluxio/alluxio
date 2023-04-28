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

package alluxio.table.under.glue;

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

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.common.FileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Glue utils.
 */
public class GlueUtils {

  private GlueUtils() {}

  /**
   * Convert glue field schema to alluxio proto (Glue do not have filedschema api).
   *
   * @param glueColumns list of glue columns
   * @return alluxio proto of schema
   */
  public static Schema toProtoSchema(List<Column> glueColumns) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.addAllCols(toProto(glueColumns));
    return schemaBuilder.build();
  }

  /**
   * Convert the Glue FieldSchema to Alluxio FieldSchema.
   *
   * @param glueCloumns Glue FiledSchema
   * @return list of Alluxio FieldSchema
   */
  public static List<alluxio.grpc.table.FieldSchema> toProto(List<Column> glueCloumns) {
    if (glueCloumns == null) {
      return Collections.emptyList();
    }
    List<alluxio.grpc.table.FieldSchema> list = new ArrayList<>();
    for (Column column:glueCloumns) {
      alluxio.grpc.table.FieldSchema.Builder builder = alluxio.grpc.table.FieldSchema.newBuilder()
          .setName(column.getName())
          .setType(column.getType());
      if (column.getComment() != null) {
        builder.setComment(column.getComment());
      }
      list.add(builder.build());
    }
    return list;
  }

  /**
   * Convert glue ColumnStatistics to Alluxio ColumnStatisticsInfo.
   *
   * @param glueColumnStatistic glue column statistic info
   * @return Alluxio ColumnStatisticsInfo
   */
  public static ColumnStatisticsInfo toProto(ColumnStatistics glueColumnStatistic) {
    if (glueColumnStatistic == null) {
      return ColumnStatisticsInfo.newBuilder().build();
    }

    ColumnStatisticsInfo.Builder columnStatisticsInfoBuilder = ColumnStatisticsInfo.newBuilder();
    columnStatisticsInfoBuilder.setColName(glueColumnStatistic.getColumnName())
        .setColType(glueColumnStatistic.getColumnType());

    if (glueColumnStatistic.getStatisticsData() != null) {
      com.amazonaws.services.glue.model.ColumnStatisticsData glueColumnStatisticsData =
          glueColumnStatistic.getStatisticsData();
      String columnType = glueColumnStatistic.getStatisticsData().getType();
      if (columnType != null) {
        if (columnType.equals("BOOLEAN")
            && glueColumnStatisticsData.getBooleanColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.BooleanColumnStatisticsData booleanData =
              glueColumnStatisticsData.getBooleanColumnStatisticsData();
          if (booleanData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setBooleanStats(toProto(booleanData)).build());
          }
        }
        if (columnType.equals("DATE")
            && glueColumnStatisticsData.getDateColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.DateColumnStatisticsData dateData =
              glueColumnStatisticsData.getDateColumnStatisticsData();
          if (dateData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setDateStats(toProto(dateData)).build());
          }
        }
        if (columnType.equals("DECIMAL")
            && glueColumnStatisticsData.getDecimalColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.DecimalColumnStatisticsData decimalData =
              glueColumnStatisticsData.getDecimalColumnStatisticsData();
          if (decimalData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setDecimalStats(toProto(decimalData)).build());
          }
        }
        if (columnType.equals("DOUBLE")
            && glueColumnStatisticsData.getDoubleColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.DoubleColumnStatisticsData doubleData =
              glueColumnStatisticsData.getDoubleColumnStatisticsData();
          if (doubleData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setDoubleStats(toProto(doubleData)).build());
          }
        }
        if (columnType.equals("LONG")
            && glueColumnStatisticsData.getLongColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.LongColumnStatisticsData longData =
              glueColumnStatisticsData.getLongColumnStatisticsData();
          if (longData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setLongStats(toProto(longData)).build());
          }
        }
        if (columnType.equals("STRING")
            && glueColumnStatisticsData.getStringColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.StringColumnStatisticsData stringData =
              glueColumnStatisticsData.getStringColumnStatisticsData();
          if (stringData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setStringStats(toProto(stringData)).build());
          }
        }
        if (columnType.equals("BINARY")
            && glueColumnStatisticsData.getBinaryColumnStatisticsData() != null) {
          com.amazonaws.services.glue.model.BinaryColumnStatisticsData binaryData =
              glueColumnStatisticsData.getBinaryColumnStatisticsData();
          if (binaryData != null) {
            columnStatisticsInfoBuilder.setData(
                ColumnStatisticsData.newBuilder().setBinaryStats(toProto(binaryData)).build());
          }
        }
      }
    }

    return columnStatisticsInfoBuilder.build();
  }

  private static BooleanColumnStatsData toProto(
      com.amazonaws.services.glue.model.BooleanColumnStatisticsData booleanData) {
    BooleanColumnStatsData.Builder builder = BooleanColumnStatsData.newBuilder();
    builder.setNumNulls(booleanData.getNumberOfNulls())
        .setNumTrues(booleanData.getNumberOfTrues())
        .setNumFalses(booleanData.getNumberOfFalses());
    return builder.build();
  }

  private static DateColumnStatsData toProto(
      com.amazonaws.services.glue.model.DateColumnStatisticsData dateData) {
    DateColumnStatsData.Builder builder = DateColumnStatsData.newBuilder();
    builder.setNumNulls(dateData.getNumberOfNulls())
        .setNumDistincts(dateData.getNumberOfDistinctValues());
    if (dateData.getMaximumValue() != null) {
      builder.setHighValue(Date.newBuilder()
          .setDaysSinceEpoch(dateData.getMaximumValue().getTime()).build());
    }
    if (dateData.getMinimumValue() != null) {
      builder.setLowValue(Date.newBuilder()
          .setDaysSinceEpoch(dateData.getMinimumValue().getTime()).build());
    }
    return builder.build();
  }

  private static DecimalColumnStatsData toProto(
      com.amazonaws.services.glue.model.DecimalColumnStatisticsData decimalData) {
    DecimalColumnStatsData.Builder builder = DecimalColumnStatsData.newBuilder();
    builder.setNumNulls(decimalData.getNumberOfNulls())
        .setNumDistincts(decimalData.getNumberOfDistinctValues());
    if (decimalData.getMaximumValue() != null) {
      builder.setHighValue(Decimal.newBuilder().setScale(decimalData.getMaximumValue().getScale())
              .setUnscaled(
                  ByteString.copyFrom(decimalData.getMaximumValue().getUnscaledValue().array())));
    }
    if (decimalData.getMinimumValue() != null) {
      builder.setLowValue(Decimal.newBuilder().setScale(decimalData.getMinimumValue().getScale())
          .setUnscaled(
              ByteString.copyFrom(decimalData.getMinimumValue().getUnscaledValue().array())));
    }
    return builder.build();
  }

  private static DoubleColumnStatsData toProto(
      com.amazonaws.services.glue.model.DoubleColumnStatisticsData doubleData) {
    DoubleColumnStatsData.Builder builder = DoubleColumnStatsData.newBuilder();
    builder.setNumNulls(doubleData.getNumberOfNulls())
        .setNumDistincts(doubleData.getNumberOfDistinctValues());
    if (doubleData.getMaximumValue() != null) {
      builder.setHighValue(doubleData.getMaximumValue());
    }
    if (doubleData.getMinimumValue() != null) {
      builder.setLowValue(doubleData.getMinimumValue());
    }
    return builder.build();
  }

  private static LongColumnStatsData toProto(
      com.amazonaws.services.glue.model.LongColumnStatisticsData longData) {
    LongColumnStatsData.Builder builder = LongColumnStatsData.newBuilder();
    builder.setNumNulls(longData.getNumberOfNulls())
        .setNumDistincts(longData.getNumberOfDistinctValues());
    if (longData.getMaximumValue() != null) {
      builder.setHighValue(longData.getMaximumValue());
    }
    if (longData.getMinimumValue() != null) {
      builder.setLowValue(longData.getMinimumValue());
    }
    return builder.build();
  }

  private static StringColumnStatsData toProto(
      com.amazonaws.services.glue.model.StringColumnStatisticsData stringData) {
    StringColumnStatsData.Builder builder = StringColumnStatsData.newBuilder();
    builder.setNumNulls(stringData.getNumberOfNulls())
        .setNumDistincts(stringData.getNumberOfDistinctValues());
    if (stringData.getAverageLength() != null) {
      builder.setAvgColLen(stringData.getAverageLength());
    }
    if (stringData.getMaximumLength() != null) {
      builder.setMaxColLen(stringData.getMaximumLength().longValue());
    }
    return builder.build();
  }

  private static BinaryColumnStatsData toProto(
      com.amazonaws.services.glue.model.BinaryColumnStatisticsData binaryData) {
    BinaryColumnStatsData.Builder builder = BinaryColumnStatsData.newBuilder();
    builder.setNumNulls(binaryData.getNumberOfNulls());
    if (binaryData.getMaximumLength() != null) {
      builder.setMaxColLen(binaryData.getMaximumLength());
    }
    if (binaryData.getAverageLength() != null) {
      builder.setAvgColLen(binaryData.getAverageLength());
    }
    return builder.build();
  }

  /**
   * Convert the Glue Storage Descriptor and Translator information to Storage.
   *
   * @param sd the glue storage descriptor
   * @param translator the glue translator
   * @return storage proto
   * @throws IOException
   */
  public static Storage toProto(StorageDescriptor sd, PathTranslator translator)
      throws IOException {
    if (sd == null) {
      return Storage.getDefaultInstance();
    }

    String serDe = sd.getSerdeInfo() == null ? null
        : sd.getSerdeInfo().getSerializationLibrary();
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

    alluxio.grpc.table.layout.hive.Storage.Builder storageBuilder =
        alluxio.grpc.table.layout.hive.Storage.newBuilder();
    List<String> bucketColumn = sd.getBucketColumns() == null
        ? Collections.emptyList() : sd.getBucketColumns();
    List<Order> orderList = sd.getSortColumns();
    List<SortingColumn> sortingColumns;
    if (orderList == null) {
      sortingColumns = Collections.emptyList();
    } else {
      sortingColumns = orderList.stream().map(
          order -> SortingColumn.newBuilder().setColumnName(order.getColumn())
              .setOrder(order.getSortOrder() == 1 ? SortingColumn.SortingOrder.ASCENDING
                  : SortingColumn.SortingOrder.DESCENDING).build())
          .collect(Collectors.toList());
    }
    return storageBuilder.setStorageFormat(formatBuilder.build())
        .setLocation(translator.toAlluxioPath(sd.getLocation()))
        .setBucketProperty(HiveBucketProperty.newBuilder().setBucketCount(sd.getNumberOfBuckets())
            .addAllBucketedBy(bucketColumn).addAllSortedBy(sortingColumns).build())
        .setSkewed(sd.getSkewedInfo() != null && (sd.getSkewedInfo().getSkewedColumnNames()) != null
            && !sd.getSkewedInfo().getSkewedColumnNames().isEmpty())
        .putAllSerdeParameters(sd.getParameters()).build();
  }

  /**
   * Align to hive makePartName, convert glue partition information to alluxio partition name.
   *
   * @param columns glue table partition keys
   * @param partitionValues glue partition values
   * @return partition name
   * @throws IOException
   */
  public static String makePartitionName(List<Column> columns, List<String> partitionValues)
      throws IOException {
    if ((columns.size() != partitionValues.size()) || columns.size() == 0) {
      String errorMesg = "Invalid partition key & values; key [";
      for (Column column : columns) {
        errorMesg += (column.getName() + ",");
      }
      errorMesg += "], values [";
      for (String partitionValue : partitionValues) {
        errorMesg += (partitionValue + ", ");
      }
      throw new IOException(errorMesg + "]");
    }
    List<String> columnNames = new ArrayList<>();
    for (Column column : columns) {
      columnNames.add(column.getName());
    }
    return makePartName(columnNames, partitionValues);
  }

  /**
   * Make partition name for glue, wrapper of hive makePartName.
   *
   * @param partCols partition columns
   * @param vals partition values
   * @return partition name
   */
  public static String makePartName(List<String> partCols, List<String> vals) {
    return FileUtils.makePartName(partCols, vals);
  }
}
