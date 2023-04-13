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

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Applies a simple Gzip compression to the {@link NoCompressionMarshaller}.
 */
public class GzipMarshaller implements DirectoryMarshaller {
  private final int mSnapshotCompressionLevel = Configuration.getInt(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_COMPRESSION_LEVEL);
  private final NoCompressionMarshaller mMarshaller = new NoCompressionMarshaller();

  @Override
  public long write(Path path, OutputStream outputStream) throws IOException, InterruptedException {
    GzipParameters params = new GzipParameters();
    params.setCompressionLevel(mSnapshotCompressionLevel);
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(outputStream, params);
    long bytes = mMarshaller.write(path, zipStream);
    zipStream.finish();
    return bytes;
  }

  @Override
  public long read(Path path, InputStream inputStream) throws IOException {
    InputStream zipStream = new GzipCompressorInputStream(inputStream);
    return mMarshaller.read(path, zipStream);
  }
}
