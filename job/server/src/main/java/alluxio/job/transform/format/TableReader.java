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

import alluxio.Constants;
import alluxio.job.transform.format.csv.CsvReader;
import alluxio.job.transform.format.parquet.ParquetReader;

import java.io.Closeable;
import java.io.IOException;

/**
 * A reader for reading rows in a table.
 */
public interface TableReader extends Closeable {
  /**
   * @param scheme the scheme of the path, e.g. "alluxio", "file"
   * @param path the path to the file representing the table
   * @return the reader for the table
   * @throws IOException when failed to create the reader
   */
  static TableReader create(String scheme, String path) throws IOException {
    if (Format.isCsv(path)) {
      return CsvReader.create(scheme, path);
    } else if (Format.isParquet(path)) {
      return ParquetReader.create(scheme, path);
    } else {
      throw new IOException("Unsupported file format for " + path);
    }
  }

  /**
   * @param path the path to the file representing the table
   * @return the result of {@link #create(String, String)} with scheme set to "alluxio"
   * @throws IOException when failed to create the reader
   */
  static TableReader create(String path) throws IOException {
    return create(Constants.SCHEME, path);
  }

  /**
   * @return the table schema
   * @throws IOException when failed to read the schema
   */
  TableSchema getSchema() throws IOException;

  /**
   * @return the next row or null if there are no more rows
   * @throws IOException when read fails
   */
  TableRow read() throws IOException;

  /**
   * Closes the reader, which will close the underlying stream.
   *
   * @throws IOException when failing to close the underlying stream
   */
  void close() throws IOException;
}
