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

import static java.util.stream.Collectors.toList;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Utility methods for working with tar archives.
 */
public final class TarUtils {
  /**
   * Creates a gzipped tar archive from the given path, streaming the data to the give output
   * stream.
   *
   * @param dirPath the path to archive
   * @param compressionLevel the compression level to use (0 for no compression, 9 for the most
   *                         compression, or -1 for system default)
   * @param output the output stream to write the data to
   * @return the number of bytes copied from the directory into the archive
   */
  public static long writeTarGz(Path dirPath, OutputStream output, int compressionLevel)
      throws IOException, InterruptedException {
    GzipParameters params = new GzipParameters();
    params.setCompressionLevel(compressionLevel);
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(output, params);
    TarArchiveOutputStream archiveStream = new TarArchiveOutputStream(zipStream);
    archiveStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    archiveStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    long totalBytesCopied = 0;
    try (final Stream<Path> stream = Files.walk(dirPath)) {
      for (Path subPath : stream.collect(toList())) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        File file = subPath.toFile();
        TarArchiveEntry entry = new TarArchiveEntry(file, dirPath.relativize(subPath).toString());
        archiveStream.putArchiveEntry(entry);
        if (file.isFile()) {
          try (InputStream fileIn = new BufferedInputStream(Files.newInputStream(subPath))) {
            totalBytesCopied += IOUtils.copyLarge(fileIn, archiveStream);
          }
        }
        archiveStream.closeArchiveEntry();
      }
    }
    archiveStream.finish();
    zipStream.finish();
    return totalBytesCopied;
  }

  /**
   * Reads a gzipped tar archive from a stream and writes it to the given path.
   *
   * @param dirPath the path to write the archive to
   * @param input the input stream
   * @return the number of bytes copied from the archive in the directory
   */
  public static long readTarGz(Path dirPath, InputStream input) throws IOException {
    InputStream zipStream = new GzipCompressorInputStream(input);
    TarArchiveInputStream archiveStream = new TarArchiveInputStream(zipStream);
    TarArchiveEntry entry;
    long totalBytesCopied = 0;
    while ((entry = (TarArchiveEntry) archiveStream.getNextEntry()) != null) {
      File outputFile = new File(dirPath.toFile(), entry.getName());
      if (entry.isDirectory()) {
        outputFile.mkdirs();
      } else {
        outputFile.getParentFile().mkdirs();
        try (OutputStream fileOut =
                 new BufferedOutputStream(Files.newOutputStream(outputFile.toPath()))) {
          totalBytesCopied += IOUtils.copyLarge(archiveStream, fileOut);
        }
      }
    }
    return totalBytesCopied;
  }

  private TarUtils() {} // Utils class
}
