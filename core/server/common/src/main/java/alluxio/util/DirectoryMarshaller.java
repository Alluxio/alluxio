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

package alluxio.util;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Interface for a directory marshaller to follow.
 */
public interface DirectoryMarshaller {
  /**
   * Writes the contents of path ot outputStream such that it can be read by
   * {@link #read(Path, InputStream)}.
   * @param path the directory to marshall
   * @param outputStream the output stream that the marshalled information
   * @return the number of bytes read in path
   */
  long write(Path path, OutputStream outputStream) throws IOException, InterruptedException;

  /**
   * Reads the content from the inputStream and writes them to the specified path.
   * @param path the output path
   * @param inputStream the stream to read the data from
   * @return the number of bytes written to path
   */
  long read(Path path, InputStream inputStream) throws IOException;

  /**
   * Factory to access the DirectoryMarshaller.
   */
  class Factory {
    /**
     * @return a {@link DirectoryMarshaller}
     */
    public static DirectoryMarshaller create() {
      int snapshotCompressionLevel =
          Configuration.getInt(PropertyKey.MASTER_METASTORE_ROCKS_CHECKPOINT_COMPRESSION_LEVEL);
      if (snapshotCompressionLevel == 0) {
        return new NoCompressionMarshaller();
      }
      return new TarGzMarshaller();
    }
  }
}
