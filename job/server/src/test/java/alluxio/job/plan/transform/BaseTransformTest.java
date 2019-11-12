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

package alluxio.job.plan.transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for unit testing the transform package.
 * Contains predefined table definition.
 */
public abstract class BaseTransformTest {
  protected static final Schema SCHEMA;
  protected static final Record RECORD;
  protected static final String[] COLUMNS = new String[]{"a", "b", "c"};
  protected static final Integer[] VALUES = new Integer[]{
      Integer.parseInt("1001", 2),
      Integer.parseInt("1100", 2),
      Integer.parseInt("0110", 2),
  };
  protected static final String ZORDER = StringUtils.leftPad("110011001100", 32 * 3, '0');

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
