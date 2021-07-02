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

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.job.plan.transform.format.parquet.ParquetWriter;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

/**
 * A writer for writing rows to a table.
 */
public interface TableWriter extends Closeable {
  /**
   * @param schema the table schema
   * @param uri the URI to the output
   * @return the writer for the output
   */
  static TableWriter create(TableSchema schema, AlluxioURI uri) throws IOException {
    return create(schema, uri, null);
  }

  /**
   * @param schema the table schema
   * @param uri the URI to the output
   * @param partitionInfo the partition info (default configuration is used if null)
   * @return the writer for the output
   */
  static TableWriter create(TableSchema schema, AlluxioURI uri,
                            @Nullable PartitionInfo partitionInfo) throws IOException {
    ReadWriterUtils.checkUri(uri);
    return ParquetWriter.create(schema, uri, partitionInfo);
  }

  /**
   * Writes a row.
   *
   * @param row a row
   * @throws IOException when write fails
   */
  void write(TableRow row) throws IOException;

  /**
   * Closes a writer, which means the table is complete now.
   *
   * @throws IOException when failing to close the underlying output stream
   */
  void close() throws IOException;

  /**
   * @return the number of rows that have been written
   */
  int getRows();

  /**
   * Note that the bytes written should take compression and encoding into consideration.
   * If the writer writes to a file, the bytes written should be an estimate of the actual bytes
   * written to the file.
   *
   * @return the number of bytes that have been written
   */
  long getBytes();
}
