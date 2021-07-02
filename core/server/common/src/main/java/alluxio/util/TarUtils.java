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

import static java.util.stream.Collectors.toList;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
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
   * @param output the output stream to write the data to
   */
  public static void writeTarGz(Path dirPath, OutputStream output)
      throws IOException, InterruptedException {
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(output);
    TarArchiveOutputStream archiveStream = new TarArchiveOutputStream(zipStream);
    try (final Stream<Path> stream = Files.walk(dirPath)) {
      for (Path subPath : stream.collect(toList())) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        File file = subPath.toFile();
        TarArchiveEntry entry = new TarArchiveEntry(file, dirPath.relativize(subPath).toString());
        archiveStream.putArchiveEntry(entry);
        if (file.isFile()) {
          try (InputStream fileIn = Files.newInputStream(subPath)) {
            IOUtils.copy(fileIn, archiveStream);
          }
        }
        archiveStream.closeArchiveEntry();
      }
    }
    archiveStream.finish();
    zipStream.finish();
  }

  /**
   * Reads a gzipped tar archive from a stream and writes it to the given path.
   *
   * @param dirPath the path to write the archive to
   * @param input the input stream
   */
  public static void readTarGz(Path dirPath, InputStream input) throws IOException {
    InputStream zipStream = new GzipCompressorInputStream(input);
    TarArchiveInputStream archiveStream = new TarArchiveInputStream(zipStream);
    TarArchiveEntry entry;
    while ((entry = (TarArchiveEntry) archiveStream.getNextEntry()) != null) {
      File outputFile = new File(dirPath.toFile(), entry.getName());
      if (entry.isDirectory()) {
        outputFile.mkdirs();
      } else {
        outputFile.getParentFile().mkdirs();
        try (FileOutputStream fileOut = new FileOutputStream(outputFile)) {
          IOUtils.copy(archiveStream, fileOut);
        }
      }
    }
  }

  private TarUtils() {} // Utils class
}
