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

import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.parquet.ParquetRow;

import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericData.Record;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * A row in a CSV table represented in Avro format.
 */
public final class CsvRow implements TableRow {
  private final Record mRecord;

  /**
   * @param record the representation of a row in a Parquet table in the Avro format
   */
  public CsvRow(@NotNull Record record) {
    mRecord = Preconditions.checkNotNull(record, "record");
  }

  @Override
  public Object getColumn(String column) {
    return mRecord.get(column);
  }

  @Override
  public ParquetRow toParquet() {
    return new ParquetRow(mRecord);
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
    return Objects.equals(mRecord, that.mRecord);
  }

  @Override
  public int hashCode() {
    return mRecord.hashCode();
  }
}
