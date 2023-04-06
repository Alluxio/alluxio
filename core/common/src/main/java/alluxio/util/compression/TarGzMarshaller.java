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

package alluxio.util.compression;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Marshall directory following the .tar.gz specification.
 */
public class TarGzMarshaller implements DirectoryMarshaller {
  private final int mSnapshotCompressionLevel = Configuration.getInt(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_COMPRESSION_LEVEL);

  @Override
  public long write(Path path, OutputStream outputStream) throws IOException, InterruptedException {
    return TarUtils.writeTarGz(path, outputStream, mSnapshotCompressionLevel);
  }

  @Override
  public long read(Path path, InputStream inputStream) throws IOException {
    return TarUtils.readTarGz(path, inputStream);
  }
}
