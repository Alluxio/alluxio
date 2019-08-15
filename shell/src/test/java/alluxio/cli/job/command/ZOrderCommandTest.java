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

package alluxio.cli.job.command;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ZOrderCommandTest {
  private static final Schema SCHEMA;
  private static final Record RECORD;
  private static final String[] COLUMNS = new String[]{"a", "b", "c"};
  private static final Integer[] VALUES = new Integer[]{
      Integer.parseInt("1001", 2),
      Integer.parseInt("1100", 2),
      Integer.parseInt("0110", 2),
  };
  private static final String ZORDER = StringUtils.leftPad("110011001100", 32 * 3, '0');

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

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Test
  public void zorder() {
    String z = ZOrderCommand.zorder(RECORD, COLUMNS);
    assertEquals(ZORDER, z);
  }

  @Test
  public void writeRecords() throws Exception {
    File file = mTempFolder.newFile();
    file.delete();
    ZOrderCommand.writeRecords(file, SCHEMA, Lists.newArrayList(RECORD));

    List<Record> records = new ArrayList<>();
    try (ParquetReader<Record> reader = ZOrderCommand.createParquetReader(file)) {
      for (Record record = reader.read(); record != null; record = reader.read()) {
        records.add(record);
      }
    }
    assertEquals(1, records.size());
    Record record = records.get(0);
    for (int i = 0; i < COLUMNS.length; i++) {
      assertEquals(VALUES[i], record.get(COLUMNS[i]));
    }
  }
}
