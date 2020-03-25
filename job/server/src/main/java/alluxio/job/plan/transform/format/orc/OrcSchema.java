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


package alluxio.job.plan.transform.format.orc;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.format.SchemaConversionUtils;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.parquet.ParquetSchema;
import org.apache.avro.Schema;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;

public class OrcSchema implements TableSchema {
  private final ArrayList<FieldSchema> mAlluxioSchema;

  private final Schema mWriteSchema;

  public OrcSchema(@NotNull ArrayList<FieldSchema> schema) throws IOException {
    mAlluxioSchema = schema;
    mWriteSchema = SchemaConversionUtils.buildWriteSchema(Schema.Type.RECORD.getName(), schema);
  }

  @Override
  public ParquetSchema toParquet() {
    return new ParquetSchema(mWriteSchema);
  }

  public ArrayList<FieldSchema> getAlluxioSchema() {
    return mAlluxioSchema;
  }

  public Schema getWriteSchema() {
    return mWriteSchema;
  }
}
