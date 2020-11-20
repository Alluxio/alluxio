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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.grpc.table.ColumnStatisticsInfo;

import alluxio.util.CommonUtils;

import com.amazonaws.services.glue.model.BinaryColumnStatisticsData;
import com.amazonaws.services.glue.model.BooleanColumnStatisticsData;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalNumber;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class GlueUtilsTest {

  private static final String COLUMN_NAME = "testColumn";
  private static final String PARTITION_VALUE_1 = "value1";
  private static final String PARTITION_VALUE_2 = "value2";
  private Random mRandom = ThreadLocalRandom.current();

  @Test
  public void toProtoSchema() {
    assertEquals(MockGlueDatabase.alluxioSchema(),
        GlueUtils.toProtoSchema(ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME))));
  }

  @Test
  public void toProto() {
    assertEquals(MockGlueDatabase.alluxioFieldSchema(),
        GlueUtils.toProto(ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME))));
  }

  @Test
  public void makePartitionName() throws IOException {
    List<Column> columns = ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME));
    List<Column> emptyColumns = ImmutableList.of();

    List<String> partitionValueList1 = ImmutableList.of(PARTITION_VALUE_1);
    List<String> partitionValueList2 = ImmutableList.of(PARTITION_VALUE_1, PARTITION_VALUE_2);

    String expectedMessageEmpty =
        "Invalid partition key & values; key [], values [value1, ]";
    String expectedMessageUneven =
        "Invalid partition key & values; key [testColumn,], values [value1, value2, ]";

    assertEquals(
        GlueUtils.makePartitionName(columns, partitionValueList1),
        GlueUtils.makePartName(ImmutableList.of(COLUMN_NAME), partitionValueList1));
    assertMakePartName(emptyColumns, partitionValueList1, expectedMessageEmpty);
    assertMakePartName(columns, partitionValueList2, expectedMessageUneven);
  }

  private static void assertMakePartName(List<Column> columns,
      List<String> partitionValues, String expectedException) {
    IOException ioException = new IOException();
    try {
      GlueUtils.makePartitionName(columns, partitionValues);
    } catch (IOException e) {
      ioException = e;
    }
    assertExecption(ioException, expectedException);
  }

  private static void assertExecption(IOException ioExecption, String expectedException) {
    assertTrue(ioExecption.getMessage().contains(expectedException));
  }

  @Test
  public void protoColStatsWithBooleanData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("BOOLEAN");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    BooleanColumnStatisticsData booleanData = new BooleanColumnStatisticsData();
    booleanData.setNumberOfFalses(mRandom.nextLong());
    booleanData.setNumberOfTrues(mRandom.nextLong());
    booleanData.setNumberOfNulls(mRandom.nextLong());
    data.setBooleanColumnStatisticsData(booleanData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithDateData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("DATE");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    DateColumnStatisticsData dateData = new DateColumnStatisticsData();
    dateData.setMaximumValue(new Date(mRandom.nextLong()));
    dateData.setMinimumValue(new Date(mRandom.nextLong()));
    dateData.setNumberOfNulls(mRandom.nextLong());
    dateData.setNumberOfDistinctValues(mRandom.nextLong());
    data.setDateColumnStatisticsData(dateData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify null column values
    dateData.setMaximumValue(null);
    dateData.setMinimumValue(null);
    data.setDateColumnStatisticsData(dateData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithDecimalData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("DECIMAL");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    DecimalColumnStatisticsData decimalData = new DecimalColumnStatisticsData();
    DecimalNumber maxDecimalNumber = new DecimalNumber();
    maxDecimalNumber.setScale(mRandom.nextInt());
    maxDecimalNumber.setUnscaledValue(ByteBuffer.wrap(CommonUtils.randomBytes(5)));
    DecimalNumber minDecimalNumber = new DecimalNumber();
    minDecimalNumber.setScale(mRandom.nextInt());
    minDecimalNumber.setUnscaledValue(ByteBuffer.wrap(CommonUtils.randomBytes(5)));
    decimalData.setMaximumValue(maxDecimalNumber);
    decimalData.setMinimumValue(minDecimalNumber);
    decimalData.setNumberOfNulls(mRandom.nextLong());
    decimalData.setNumberOfDistinctValues(mRandom.nextLong());
    data.setDecimalColumnStatisticsData(decimalData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify null column values
    decimalData.setMaximumValue(null);
    decimalData.setMinimumValue(null);
    data.setDecimalColumnStatisticsData(decimalData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithDoubleData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("DOUBLE");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    DoubleColumnStatisticsData doubleData = new DoubleColumnStatisticsData();
    doubleData.setMaximumValue(mRandom.nextDouble());
    doubleData.setMinimumValue(mRandom.nextDouble());
    doubleData.setNumberOfNulls(mRandom.nextLong());
    doubleData.setNumberOfDistinctValues(mRandom.nextLong());
    data.setDoubleColumnStatisticsData(doubleData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify null column values
    doubleData.setMaximumValue(null);
    doubleData.setMinimumValue(null);
    data.setDoubleColumnStatisticsData(doubleData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithLongData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("LONG");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    LongColumnStatisticsData longData = new LongColumnStatisticsData();
    longData.setMaximumValue(mRandom.nextLong());
    longData.setMinimumValue(mRandom.nextLong());
    longData.setNumberOfNulls(mRandom.nextLong());
    longData.setNumberOfDistinctValues(mRandom.nextLong());
    data.setLongColumnStatisticsData(longData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify null column values
    longData.setMaximumValue(null);
    longData.setMinimumValue(null);
    data.setLongColumnStatisticsData(longData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithStringData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("STRING");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    StringColumnStatisticsData stringData = new StringColumnStatisticsData();
    stringData.setMaximumLength(mRandom.nextLong());
    stringData.setAverageLength(mRandom.nextDouble());
    stringData.setNumberOfNulls(mRandom.nextLong());
    stringData.setNumberOfDistinctValues(mRandom.nextLong());
    data.setStringColumnStatisticsData(stringData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  @Test
  public void protoColStatsWithBinaryData() {
    // ColumnStatistics required fields: AnalyzedTime, ColumnName, ColumnType, StatisticsData
    ColumnStatistics glueColStats = new ColumnStatistics();
    glueColStats.setColumnName("colName");
    glueColStats.setColumnType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    data.setType("BINARY");
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);

    // verify non-empty data
    BinaryColumnStatisticsData binaryData = new BinaryColumnStatisticsData();
    binaryData.setAverageLength(mRandom.nextDouble());
    binaryData.setMaximumLength(mRandom.nextLong());
    binaryData.setNumberOfNulls(mRandom.nextLong());
    data.setBinaryColumnStatisticsData(binaryData);
    glueColStats.setStatisticsData(data);
    verifyColumnStats(glueColStats);
  }

  private void verifyColumnStats(ColumnStatistics glueColStats) {
    ColumnStatisticsInfo colStats = GlueUtils.toProto(glueColStats);
    assertEquals(glueColStats.getColumnName(), colStats.getColName());
    assertEquals(glueColStats.getColumnType(), colStats.getColType());

    // verify empty ColumnStatisticData
    if (glueColStats.getStatisticsData() == null) {
      assertEquals(glueColStats.getStatisticsData() == null
              && glueColStats.getStatisticsData().getType() != null,
          colStats.hasData());
    }

    if (glueColStats.getStatisticsData() != null) {
      ColumnStatisticsData glueData = glueColStats.getStatisticsData();
      alluxio.grpc.table.ColumnStatisticsData data = colStats.getData();

      //verify boolean
      if (glueData.getBooleanColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "BOOLEAN");
        BooleanColumnStatisticsData glueBoolean = glueData.getBooleanColumnStatisticsData();
        assertEquals(glueBoolean.getNumberOfFalses() != null
                && glueBoolean.getNumberOfTrues() != null
                && glueBoolean.getNumberOfNulls() != null,
            data.hasBooleanStats());
        if (data.hasBooleanStats()) {
          alluxio.grpc.table.BooleanColumnStatsData boolData = data.getBooleanStats();
          assertEquals(glueBoolean.getNumberOfFalses().longValue(), boolData.getNumFalses());
          assertEquals(glueBoolean.getNumberOfTrues().longValue(), boolData.getNumTrues());
          assertEquals(glueBoolean.getNumberOfNulls().longValue(), boolData.getNumNulls());
        }
      }

      //verify date
      if (glueData.getDateColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "DATE");
        DateColumnStatisticsData glueDate = glueData.getDateColumnStatisticsData();
        assertEquals(glueDate.getNumberOfDistinctValues() != null
                && glueDate.getNumberOfNulls() != null,
            data.hasDateStats());
        if (data.hasDateStats()) {
          alluxio.grpc.table.DateColumnStatsData date = data.getDateStats();
          assertEquals(glueDate.getNumberOfDistinctValues().longValue(), date.getNumDistincts());
          assertEquals(glueDate.getNumberOfNulls().longValue(), date.getNumNulls());
          assertEquals(glueDate.getMaximumValue() != null, date.hasHighValue());
          if (glueDate.getMaximumValue() != null) {
            assertEquals(glueDate.getMaximumValue().getTime(),
                date.getHighValue().getDaysSinceEpoch());
          }
          assertEquals(glueDate.getMinimumValue() != null, date.hasLowValue());
          if (glueDate.getMinimumValue() != null) {
            assertEquals(glueDate.getMinimumValue().getTime(),
                date.getLowValue().getDaysSinceEpoch());
          }
        }
      }

      //verify decimal
      if (glueData.getDecimalColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "DECIMAL");
        DecimalColumnStatisticsData glueDecimal = glueData.getDecimalColumnStatisticsData();
        assertEquals(glueDecimal.getNumberOfDistinctValues() != null
                && glueDecimal.getNumberOfNulls() != null,
            data.hasDecimalStats());
        if (data.hasDecimalStats()) {
          alluxio.grpc.table.DecimalColumnStatsData decimal = data.getDecimalStats();
          assertEquals(glueDecimal.getNumberOfDistinctValues().longValue(),
              decimal.getNumDistincts());
          assertEquals(glueDecimal.getNumberOfNulls().longValue(), decimal.getNumNulls());
          assertEquals(glueDecimal.getMaximumValue() != null, decimal.hasHighValue());
          if (glueDecimal.getMaximumValue() != null) {
            assertEquals(glueDecimal.getMaximumValue().getScale().longValue(),
                decimal.getHighValue().getScale());
            assertArrayEquals(glueDecimal.getMaximumValue().getUnscaledValue().array(),
                decimal.getHighValue().getUnscaled().toByteArray());
          }
          assertEquals(glueDecimal.getMinimumValue() != null, decimal.hasLowValue());
          if (glueDecimal.getMinimumValue() != null) {
            assertEquals(glueDecimal.getMinimumValue().getScale().longValue(),
                decimal.getLowValue().getScale());
            assertArrayEquals(glueDecimal.getMinimumValue().getUnscaledValue().array(),
                decimal.getLowValue().getUnscaled().toByteArray());
          }
        }
      }

      //verify double
      if (glueData.getDoubleColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "DOUBLE");
        DoubleColumnStatisticsData glueDouble = glueData.getDoubleColumnStatisticsData();
        assertEquals(glueDouble.getNumberOfDistinctValues() != null
                && glueDouble.getNumberOfNulls() != null,
            data.hasDoubleStats());
        if (data.hasDoubleStats()) {
          alluxio.grpc.table.DoubleColumnStatsData doubleData = data.getDoubleStats();
          assertEquals(glueDouble.getNumberOfDistinctValues().longValue(),
              doubleData.getNumDistincts());
          assertEquals(glueDouble.getNumberOfNulls().longValue(), doubleData.getNumNulls());
          assertEquals(glueDouble.getMaximumValue() != null, doubleData.hasHighValue());
          if (glueDouble.getMaximumValue() != null) {
            assertEquals(glueDouble.getMaximumValue().doubleValue(),
                doubleData.getHighValue(), 0.01);
          }
          assertEquals(glueDouble.getMinimumValue() != null, doubleData.hasLowValue());
          if (glueDouble.getMinimumValue() != null) {
            assertEquals(glueDouble.getMinimumValue().doubleValue(),
                doubleData.getLowValue(), 0.01);
          }
        }
      }

      //verify long
      if (glueData.getLongColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "LONG");
        LongColumnStatisticsData glueLong = glueData.getLongColumnStatisticsData();
        assertEquals(glueLong.getNumberOfDistinctValues() != null
                && glueLong.getNumberOfNulls() != null,
            data.hasLongStats());
        if (data.hasLongStats()) {
          alluxio.grpc.table.LongColumnStatsData longData = data.getLongStats();
          assertEquals(glueLong.getNumberOfDistinctValues().longValue(),
              longData.getNumDistincts());
          assertEquals(glueLong.getNumberOfNulls().longValue(), longData.getNumNulls());
          assertEquals(glueLong.getMaximumValue() != null, longData.hasHighValue());
          if (glueLong.getMaximumValue() != null) {
            assertEquals(glueLong.getMaximumValue().longValue(), longData.getHighValue());
          }
          assertEquals(glueLong.getMinimumValue() != null, longData.hasLowValue());
          if (glueLong.getMinimumValue() != null) {
            assertEquals(glueLong.getMinimumValue().longValue(), longData.getLowValue());
          }
        }
      }

      //verify string
      if (glueData.getStringColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "STRING");
        StringColumnStatisticsData glueString = glueData.getStringColumnStatisticsData();
        assertEquals(glueString.getNumberOfDistinctValues() != null
                && glueString.getNumberOfNulls() != null
                && glueString.getMaximumLength() != null
                && glueString.getAverageLength() != null,
            data.hasStringStats());
        if (data.hasStringStats()) {
          alluxio.grpc.table.StringColumnStatsData stringData = data.getStringStats();
          assertEquals(glueString.getNumberOfDistinctValues().longValue(),
              stringData.getNumDistincts());
          assertEquals(glueString.getNumberOfNulls().longValue(), stringData.getNumNulls());
          assertEquals(glueString.getMaximumLength().longValue(), stringData.getMaxColLen());
          assertEquals(glueString.getAverageLength().doubleValue(),
              stringData.getAvgColLen(), 0.01);
        }
      }

      //verify binary
      if (glueData.getBinaryColumnStatisticsData() != null) {
        assertEquals(glueData.getType(), "BINARY");
        BinaryColumnStatisticsData glueBinary = glueData.getBinaryColumnStatisticsData();
        assertEquals(glueBinary.getAverageLength() != null
                && glueBinary.getMaximumLength() != null
                && glueBinary.getNumberOfNulls() != null,
            data.hasBinaryStats());
        if (data.hasBinaryStats()) {
          alluxio.grpc.table.BinaryColumnStatsData binary = data.getBinaryStats();
          assertEquals(glueBinary.getAverageLength().doubleValue(), binary.getAvgColLen(), 0.01);
          assertEquals(glueBinary.getMaximumLength().longValue(), binary.getMaxColLen());
          assertEquals(glueBinary.getNumberOfNulls().longValue(), binary.getNumNulls());
        }
      }
    }
  }
}
