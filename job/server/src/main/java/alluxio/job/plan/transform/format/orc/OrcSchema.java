/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
