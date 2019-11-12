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

package alluxio.job.plan.transform.format.parquet;

import alluxio.job.plan.transform.format.TableRow;

import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericData.Record;

import java.io.IOException;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * A row in a Parquet table represented in Avro format.
 */
public final class ParquetRow implements TableRow {
  private final Record mRecord;

  /**
   * @param record the representation of a row in a Parquet table in the Avro format
   */
  public ParquetRow(@NotNull Record record) {
    mRecord = Preconditions.checkNotNull(record, "record");
  }

  @Override
  public Object getColumn(String column) {
    return mRecord.get(column);
  }

  /**
   * @return the row represented in Avro format
   */
  public Record getRecord() {
    return mRecord;
  }

  @Override
  public ParquetRow toParquet() throws IOException  {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ParquetRow)) {
      return false;
    }
    ParquetRow that = (ParquetRow) o;
    return Objects.equals(mRecord, that.mRecord);
  }

  @Override
  public int hashCode() {
    return mRecord.hashCode();
  }
}
