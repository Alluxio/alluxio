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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.util.CommonUtils;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class HiveUtilsTest {

  private Random mRandom = ThreadLocalRandom.current();

  @Test
  public void protoColStatsNoData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");
    verifyColumnStats(hiveColStats);
  }

  //    data.setStringStats();

  @Test
  public void protoColStatsWithBinaryData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
    data.setBinaryStats(binaryData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    binaryData.setAvgColLen(mRandom.nextDouble());
    binaryData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    binaryData.setMaxColLen(mRandom.nextLong());
    binaryData.setNumNulls(mRandom.nextLong());
    data.setBinaryStats(binaryData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithBooleanData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    BooleanColumnStatsData booleanData = new BooleanColumnStatsData();
    data.setBooleanStats(booleanData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    booleanData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    booleanData.setNumNulls(mRandom.nextLong());
    booleanData.setNumFalses(mRandom.nextLong());
    booleanData.setNumTrues(mRandom.nextLong());
    data.setBooleanStats(booleanData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithDateData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    DateColumnStatsData dateData = new DateColumnStatsData();
    data.setDateStats(dateData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    dateData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    dateData.setNumNulls(mRandom.nextLong());
    dateData.setHighValue(new Date(mRandom.nextLong()));
    dateData.setLowValue(new Date(mRandom.nextLong()));
    dateData.setNumDVs(mRandom.nextLong());
    data.setDateStats(dateData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify null column values
    dateData.setHighValue(null);
    dateData.setLowValue(null);
    data.setDateStats(dateData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithDecimalData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
    data.setDecimalStats(decimalData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    decimalData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    decimalData.setNumNulls(mRandom.nextLong());
    decimalData.setHighValue(
        new Decimal(ByteBuffer.wrap(CommonUtils.randomBytes(5)), (short) mRandom.nextInt()));
    decimalData.setLowValue(
        new Decimal(ByteBuffer.wrap(CommonUtils.randomBytes(5)), (short) mRandom.nextInt()));
    decimalData.setNumDVs(mRandom.nextLong());
    data.setDecimalStats(decimalData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify null column values
    decimalData.setHighValue(null);
    decimalData.setLowValue(null);
    data.setDecimalStats(decimalData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithDoubleData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    data.setDoubleStats(doubleData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    doubleData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    doubleData.setNumNulls(mRandom.nextLong());
    doubleData.setHighValue(mRandom.nextDouble());
    doubleData.setLowValue(mRandom.nextDouble());
    doubleData.setNumDVs(mRandom.nextLong());
    data.setDoubleStats(doubleData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithLongData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    LongColumnStatsData longData = new LongColumnStatsData();
    data.setLongStats(longData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    longData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    longData.setNumNulls(mRandom.nextLong());
    longData.setHighValue(mRandom.nextLong());
    longData.setLowValue(mRandom.nextLong());
    longData.setNumDVs(mRandom.nextLong());
    data.setLongStats(longData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  @Test
  public void protoColStatsWithStringData() {
    ColumnStatisticsObj hiveColStats = new ColumnStatisticsObj();
    hiveColStats.setColName("colName");
    hiveColStats.setColType("colType");

    ColumnStatisticsData data = new ColumnStatisticsData();

    // verify empty data
    StringColumnStatsData stringData = new StringColumnStatsData();
    data.setStringStats(stringData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);

    // verify non-empty data
    stringData.setBitVectors(CommonUtils.randomAlphaNumString(5));
    stringData.setNumNulls(mRandom.nextLong());
    stringData.setNumDVs(mRandom.nextLong());
    stringData.setAvgColLen(mRandom.nextDouble());
    stringData.setMaxColLen(mRandom.nextLong());
    data.setStringStats(stringData);
    hiveColStats.setStatsData(data);
    verifyColumnStats(hiveColStats);
  }

  private void verifyColumnStats(ColumnStatisticsObj hiveColStats) {
    ColumnStatisticsInfo colStats = HiveUtils.toProto(hiveColStats);
    assertEquals(hiveColStats.getColName(), colStats.getColName());
    assertEquals(hiveColStats.getColType(), colStats.getColType());
    assertEquals(hiveColStats.isSetStatsData(), colStats.hasData());
    if (hiveColStats.isSetStatsData()) {
      ColumnStatisticsData hiveData = hiveColStats.getStatsData();
      alluxio.grpc.table.ColumnStatisticsData data = colStats.getData();

      // verify binary
      assertEquals(hiveData.isSetBinaryStats(), data.hasBinaryStats());
      if (hiveData.isSetBinaryStats()) {
        BinaryColumnStatsData hiveBinary = hiveData.getBinaryStats();
        alluxio.grpc.table.BinaryColumnStatsData binary = data.getBinaryStats();
        assertEquals(hiveBinary.isSetBitVectors(), binary.hasBitVectors());
        if (hiveBinary.isSetBitVectors()) {
          assertEquals(hiveBinary.getBitVectors(), binary.getBitVectors());
        }
        assertEquals(hiveBinary.getAvgColLen(), binary.getAvgColLen(), 0.01);
        assertEquals(hiveBinary.getMaxColLen(), binary.getMaxColLen());
        assertEquals(hiveBinary.getNumNulls(), binary.getNumNulls());
      }

      // verify boolean
      assertEquals(hiveData.isSetBooleanStats(), data.hasBooleanStats());
      if (hiveData.isSetBooleanStats()) {
        BooleanColumnStatsData hiveBoolean = hiveData.getBooleanStats();
        alluxio.grpc.table.BooleanColumnStatsData bool = data.getBooleanStats();
        assertEquals(hiveBoolean.isSetBitVectors(), bool.hasBitVectors());
        if (hiveBoolean.isSetBitVectors()) {
          assertEquals(hiveBoolean.getBitVectors(), bool.getBitVectors());
        }
        assertEquals(hiveBoolean.getNumFalses(), bool.getNumFalses());
        assertEquals(hiveBoolean.getNumTrues(), bool.getNumTrues());
        assertEquals(hiveBoolean.getNumNulls(), bool.getNumNulls());
      }

      // verify date
      assertEquals(hiveData.isSetDateStats(), data.hasDateStats());
      if (hiveData.isSetDateStats()) {
        DateColumnStatsData hiveDate = hiveData.getDateStats();
        alluxio.grpc.table.DateColumnStatsData date = data.getDateStats();
        assertEquals(hiveDate.isSetBitVectors(), date.hasBitVectors());
        if (hiveDate.isSetBitVectors()) {
          assertEquals(hiveDate.getBitVectors(), date.getBitVectors());
        }
        assertEquals(hiveDate.getNumNulls(), date.getNumNulls());
        assertEquals(hiveDate.getNumDVs(), date.getNumDistincts());
        assertEquals(hiveDate.isSetHighValue(), date.hasHighValue());
        if (hiveDate.isSetHighValue()) {
          assertEquals(hiveDate.getHighValue().getDaysSinceEpoch(),
              date.getHighValue().getDaysSinceEpoch());
        }
        assertEquals(hiveDate.isSetLowValue(), date.hasLowValue());
        if (hiveDate.isSetLowValue()) {
          assertEquals(hiveDate.getLowValue().getDaysSinceEpoch(),
              date.getLowValue().getDaysSinceEpoch());
        }
      }

      // verify decimal
      assertEquals(hiveData.isSetDecimalStats(), data.hasDecimalStats());
      if (hiveData.isSetDecimalStats()) {
        DecimalColumnStatsData hiveDecimal = hiveData.getDecimalStats();
        alluxio.grpc.table.DecimalColumnStatsData decimal = data.getDecimalStats();
        assertEquals(hiveDecimal.isSetBitVectors(), decimal.hasBitVectors());
        if (hiveDecimal.isSetBitVectors()) {
          assertEquals(hiveDecimal.getBitVectors(), decimal.getBitVectors());
        }
        assertEquals(hiveDecimal.getNumNulls(), decimal.getNumNulls());
        assertEquals(hiveDecimal.getNumDVs(), decimal.getNumDistincts());
        assertEquals(hiveDecimal.isSetHighValue(), decimal.hasHighValue());
        if (hiveDecimal.isSetHighValue()) {
          assertEquals(hiveDecimal.getHighValue().getScale(), decimal.getHighValue().getScale());
          assertArrayEquals(hiveDecimal.getHighValue().getUnscaled(),
              decimal.getHighValue().getUnscaled().toByteArray());
        }
        assertEquals(hiveDecimal.isSetLowValue(), decimal.hasLowValue());
        if (hiveDecimal.isSetLowValue()) {
          assertEquals(hiveDecimal.getLowValue().getScale(), decimal.getLowValue().getScale());
          assertArrayEquals(hiveDecimal.getLowValue().getUnscaled(),
              decimal.getLowValue().getUnscaled().toByteArray());
        }
      }

      // verify double
      assertEquals(hiveData.isSetDoubleStats(), data.hasDoubleStats());
      if (hiveData.isSetDoubleStats()) {
        DoubleColumnStatsData hiveDouble = hiveData.getDoubleStats();
        alluxio.grpc.table.DoubleColumnStatsData dbl = data.getDoubleStats();
        assertEquals(hiveDouble.isSetBitVectors(), dbl.hasBitVectors());
        if (hiveDouble.isSetBitVectors()) {
          assertEquals(hiveDouble.getBitVectors(), dbl.getBitVectors());
        }
        assertEquals(hiveDouble.getNumNulls(), dbl.getNumNulls());
        assertEquals(hiveDouble.getNumDVs(), dbl.getNumDistincts());
        assertEquals(hiveDouble.isSetHighValue(), dbl.hasHighValue());
        if (hiveDouble.isSetHighValue()) {
          assertEquals(hiveDouble.getHighValue(), dbl.getHighValue(), 0.01);
        }
        assertEquals(hiveDouble.isSetLowValue(), dbl.hasLowValue());
        if (hiveDouble.isSetLowValue()) {
          assertEquals(hiveDouble.getLowValue(), dbl.getLowValue(), 0.01);
        }
      }

      // verify long
      assertEquals(hiveData.isSetLongStats(), data.hasLongStats());
      if (hiveData.isSetLongStats()) {
        LongColumnStatsData hiveLong = hiveData.getLongStats();
        alluxio.grpc.table.LongColumnStatsData dbl = data.getLongStats();
        assertEquals(hiveLong.isSetBitVectors(), dbl.hasBitVectors());
        if (hiveLong.isSetBitVectors()) {
          assertEquals(hiveLong.getBitVectors(), dbl.getBitVectors());
        }
        assertEquals(hiveLong.getNumNulls(), dbl.getNumNulls());
        assertEquals(hiveLong.getNumDVs(), dbl.getNumDistincts());
        assertEquals(hiveLong.isSetHighValue(), dbl.hasHighValue());
        if (hiveLong.isSetHighValue()) {
          assertEquals(hiveLong.getHighValue(), dbl.getHighValue());
        }
        assertEquals(hiveLong.isSetLowValue(), dbl.hasLowValue());
        if (hiveLong.isSetLowValue()) {
          assertEquals(hiveLong.getLowValue(), dbl.getLowValue());
        }
      }

      // verify string
      assertEquals(hiveData.isSetStringStats(), data.hasStringStats());
      if (hiveData.isSetStringStats()) {
        StringColumnStatsData hiveString = hiveData.getStringStats();
        alluxio.grpc.table.StringColumnStatsData string = data.getStringStats();
        assertEquals(hiveString.isSetBitVectors(), string.hasBitVectors());
        if (hiveString.isSetBitVectors()) {
          assertEquals(hiveString.getBitVectors(), string.getBitVectors());
        }
        assertEquals(hiveString.getAvgColLen(), string.getAvgColLen(), 0.01);
        assertEquals(hiveString.getMaxColLen(), string.getMaxColLen());
        assertEquals(hiveString.getNumNulls(), string.getNumNulls());
        assertEquals(hiveString.getNumDVs(), string.getNumDistincts());
      }
    }
  }
}
