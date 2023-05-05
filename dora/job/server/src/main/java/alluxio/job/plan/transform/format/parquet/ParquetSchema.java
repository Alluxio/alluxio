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

import alluxio.job.plan.transform.format.TableSchema;

import com.google.common.base.Objects;
import org.apache.avro.Schema;

/**
 * Parquet table schema in Avro format.
 */
public final class ParquetSchema implements TableSchema {
  private final Schema mSchema;

  /**
   * @param schema the table schema in Avro format
   */
  public ParquetSchema(Schema schema) {
    mSchema = schema;
  }

  /**
   * @return the schema
   */
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public ParquetSchema toParquet() {
    return this;
  }

  @Override
  public int hashCode() {
    return mSchema.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ParquetSchema)) {
      return false;
    }
    ParquetSchema that = (ParquetSchema) o;
    return Objects.equal(mSchema, that.mSchema);
  }
}
