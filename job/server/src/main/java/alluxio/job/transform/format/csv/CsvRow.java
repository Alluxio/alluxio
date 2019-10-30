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

package alluxio.job.transform.format.csv;

import alluxio.job.transform.HiveConstants;
import alluxio.job.transform.FieldSchema;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.parquet.ParquetRow;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;

import javax.validation.constraints.NotNull;

/**
 * A row in a CSV table represented in Avro format.
 */
public final class CsvRow implements TableRow {
  private final CsvSchema mSchema;
  private final Record mRecord;

  /**
   * @param schema the CSV schema
   * @param record the representation of a row in a Parquet table in the Avro format
   */
  public CsvRow(@NotNull CsvSchema schema, @NotNull Record record) {
    mSchema = Preconditions.checkNotNull(schema, "schema");
    mRecord = Preconditions.checkNotNull(record, "record");
  }

  @Override
  public Object getColumn(String column) {
    return mRecord.get(column);
  }

  @Override
  public ParquetRow toParquet() throws IOException {
    Schema writeSchema = mSchema.getWriteSchema();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(writeSchema);
    for (FieldSchema field : mSchema.getAlluxioSchema()) {
      String name = field.getName();
      String type = field.getType();
      Object value = mRecord.get(name);
      value = convert(value, name, type);
      recordBuilder.set(writeSchema.getField(name), value);
    }
    return new ParquetRow(recordBuilder.build());
  }

  // TODO(cc): improve performance since it's called for every row
  private BigDecimal parseDecimal(String v, int scale) {
    int scale = decimalSpec.getScale();
    int pointIndex = v.indexOf('.');
    int fractionLen = v.length() - pointIndex - 1;
    if (fractionLen >= scale) {
      v = v.substring(0, v.length() - (fractionLen - scale));
    } else {
      v = StringUtils.rightPad(v, scale - fractionLen, '0');
    }
    return new BigDecimal(v);
  }

  /**
   * @param value the value read based on the read schema
   * @param name the name of the field
   * @param type the type of the value based on Alluxio table schema
   * @return the value in format of the write schema
   * @throws IOException when conversion failed
   */
  private Object convert(Object value, String name, String type) throws IOException {
    if (!CsvUtils.isReadWriteTypeInconsistent(type)) {
      return value;
    }

    // Value is read from CSV as a string.
    String v = (String) value;

    // Interpretation of the string is based on the following documents:
    //
    // cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-dates
    // github.com/apache/parquet-format/blob/master/LogicalTypes.md

    if (type.startsWith(HiveConstants.Types.DECIMAL)) {
      return v.getBytes();
      // CSV: 12.34, precision=2, scale=4
      // Parquet: byte[] representation of number 123400
      // TODO(cc): save cost of this parsing since it's called for every row
//      Decimal decimal = new Decimal(type);
//      return parseDecimal(v, decimal.getScale()).unscaledValue().toByteArray();
    }
    if (type.equals(HiveConstants.Types.BINARY)) {
      // CSV: text format
      // Parquet: Binary field values are encoded to string in UTF-8?
      return v.getBytes(StandardCharsets.UTF_8);
    }
    if (type.equals(HiveConstants.Types.DATE)) {
      // CSV: 2019-01-02
      // Parquet: days from the Unix epoch
      try {
        return LocalDate.parse(v).toEpochDay();
      } catch (Throwable e) {
        throw new IOException("Failed to parse '" + v + "' as DATE: " + e);
      }
    }
    if (type.equals(HiveConstants.Types.TIMESTAMP)) {
      // CSV: 2019-10-29 10:17:42.338
      // Parquet: milliseconds from the Unix epoch
      try {
        return Timestamp.valueOf(v).getTime();
      } catch (Throwable e) {
        throw new IOException("Failed to parse '" + v + "' as TIMESTAMP: " + e);
      }
    }
    throw new IOException("Unsupported type " + type + " for field " + name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CsvRow)) {
      return false;
    }
    CsvRow that = (CsvRow) o;
    return Objects.equal(mRecord, that.mRecord)
        && Objects.equal(mSchema, that.mSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecord, mSchema);
  }
}
