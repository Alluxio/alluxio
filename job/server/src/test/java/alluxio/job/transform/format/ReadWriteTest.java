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

package alluxio.job.transform.format;

import static org.junit.Assert.assertEquals;

import alluxio.job.transform.BaseTransformTest;
import alluxio.job.transform.format.csv.CsvReader;
import alluxio.job.transform.format.parquet.ParquetReader;
import alluxio.job.transform.format.parquet.ParquetRow;
import alluxio.job.transform.format.parquet.ParquetSchema;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Tests {@link TableReader} and {@link TableWriter}.
 */
public final class ReadWriteTest extends BaseTransformTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private void createCsvFile(File file, boolean gzipped) throws IOException {
    OutputStream outputStream = new FileOutputStream(file);
    try {
      if (gzipped) {
        outputStream = new GZIPOutputStream(outputStream);
      }
      outputStream.write("a,b,c\n".getBytes());
      outputStream.write("1,2,3\n".getBytes());
    } finally {
      outputStream.close();
    }
  }

  private void createParquetFile(File file) throws IOException {
    TableSchema schema = new ParquetSchema(SCHEMA);
    TableRow row = new ParquetRow(RECORD);
    try (TableWriter writer = TableWriter.create(schema, "file", file.getPath())) {
      writer.write(row);
    }
  }

  @Test
  public void create() throws IOException {
    Map<String, Class> fileToReader = new HashMap<String, Class>() {
      {
        put("test.csv", CsvReader.class);
        put("test.csv.gz", CsvReader.class);
        put("test.parquet", ParquetReader.class);
        put("test.sorted.parquet", ParquetReader.class);
      }
    };

    for (Map.Entry<String, Class> entry : fileToReader.entrySet()) {
      String fileName = entry.getKey();
      Class cls = entry.getValue();

      File file = mTempFolder.newFile(fileName);
      Files.delete(file.toPath());
      Format format = Format.of(fileName);
      switch (format) {
        case CSV:
          createCsvFile(file, false);
          break;
        case GZIP_CSV:
          createCsvFile(file, true);
          break;
        case PARQUET:
          createParquetFile(file);
          break;
        default:
          throw new IOException("Unsupported format: " + format);
      }

      try (TableReader reader = TableReader.create("file", file.getPath())) {
        assertEquals(cls, reader.getClass());
      }
    }
  }

  @Test
  public void readWrite() throws Exception {
    final File file = mTempFolder.newFile("test.parquet");
    Files.delete(file.toPath());
    final int numRows = 10;

    TableSchema schema = new ParquetSchema(SCHEMA);
    TableRow row = new ParquetRow(RECORD);
    try (TableWriter writer = TableWriter.create(schema, "file", file.getPath())) {
      for (int r = 0; r < numRows; r++) {
        writer.write(row);
      }
    }

    List<TableRow> rows = Lists.newArrayList();
    try (TableReader reader = TableReader.create("file", file.getPath())) {
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
