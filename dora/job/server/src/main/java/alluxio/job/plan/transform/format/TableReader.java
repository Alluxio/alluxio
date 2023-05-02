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
import alluxio.job.plan.transform.Format;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.job.plan.transform.format.csv.CsvReader;
import alluxio.job.plan.transform.format.orc.OrcReader;
import alluxio.job.plan.transform.format.parquet.ParquetReader;

import java.io.Closeable;
import java.io.IOException;

/**
 * A reader for reading rows in a table.
 */
public interface TableReader extends Closeable {
  /**
   * @param uri the URI to the input
   * @param pInfo the partition info from catalog service
   * @return the reader for the input
   * @throws IOException when failed to create the reader
   */
  static TableReader create(AlluxioURI uri, PartitionInfo pInfo) throws IOException {
    ReadWriterUtils.checkUri(uri);
    Format format = pInfo.getFormat(uri.getName());
    switch (format) {
      case CSV:
        // fall through
      case GZIP_CSV:
        return CsvReader.create(uri, pInfo);
      case PARQUET:
        return ParquetReader.create(uri);
      case ORC:
        return OrcReader.create(uri);
      default:
        throw new IOException("Unsupported format: " + format);
    }
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
