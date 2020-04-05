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

package alluxio.job.plan.transform.format;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.BaseTransformTest;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.job.plan.transform.format.parquet.ParquetRow;
import alluxio.job.plan.transform.format.parquet.ParquetSchema;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Tests {@link TableReader} and {@link TableWriter}.
 */
public final class ReadWriteTest extends BaseTransformTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private PartitionInfo mPartitionInfo = new PartitionInfo(HiveConstants.PARQUET_SERDE_CLASS,
      HiveConstants.PARQUET_INPUT_FORMAT_CLASS, new HashMap<>(), new HashMap<>(),
      new ArrayList<>());

  @Test
  public void readWrite() throws Exception {
    final File file = mTempFolder.newFile("test.parquet");
    Files.delete(file.toPath());
    final int numRows = 10;

    TableSchema schema = new ParquetSchema(SCHEMA);
    TableRow row = new ParquetRow(RECORD);
    AlluxioURI uri = new AlluxioURI("file:///" + file.getPath());
    try (TableWriter writer = TableWriter.create(schema, uri, mPartitionInfo)) {
      for (int r = 0; r < numRows; r++) {
        writer.write(row);
      }
    }

    List<TableRow> rows = Lists.newArrayList();
    uri = new AlluxioURI("file:///" + file.getPath());
    try (TableReader reader = TableReader.create(uri, mPartitionInfo)) {
      assertEquals(schema, reader.getSchema());
      for (TableRow r = reader.read(); r != null; r = reader.read()) {
        rows.add(r);
      }
    }

    assertEquals(numRows, rows.size());
    for (TableRow r : rows) {
      assertEquals(row, r);
      for (int i = 0; i < COLUMNS.length; i++) {
        assertEquals(VALUES[i], r.getColumn(COLUMNS[i]));
      }
    }
  }
}
