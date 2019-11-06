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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * Test utils related to parquet files.
 */
public class ParquetTestUtils {
  private ParquetTestUtils() {} // Prevent initialization

  /** Parquet Schema. */
  public static final Schema SCHEMA;
  /** Parquet record. */
  public static final Record RECORD;
  /** Parquet columns. */
  public static final String[] COLUMNS = new String[]{"a", "b", "c"};
  /** Parquet column values. */
  public static final Integer[] VALUES = new Integer[]{1, 2, 3};

  static {
    List<Schema.Field> fields = new ArrayList<>(COLUMNS.length);
    for (String column : COLUMNS) {
      fields.add(new Schema.Field(column, Schema.create(Schema.Type.INT), null, null));
    }
    SCHEMA = Schema.createRecord("schema", null, null, false, fields);
    RECORD = new Record(SCHEMA);
    for (int i = 0; i < COLUMNS.length; i++) {
      RECORD.put(COLUMNS[i], VALUES[i]);
    }
  }
}
