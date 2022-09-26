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

import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceUtils;
import alluxio.util.io.FileUtils;

import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * Utility methods for working with parallel zip archives.
 */
public class ParallelZipUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelZipUtils.class);

  /**
   * Creates a zipped archive from the given path in parallel, streaming the data
   * to the give output stream.
   *
   * @param dirPath
   * @param outputStream
   * @param poolSize
   * @param compressionLevel
   * @throws IOException
   * @throws InterruptedException
   */
  public static void compress(Path dirPath, OutputStream outputStream, int poolSize,
        int compressionLevel)
      throws IOException, InterruptedException {
    LOG.info("compress in parallel for path {}", dirPath);
    ExecutorService executor = ExecutorServiceFactories.fixedThreadPool(
        "parallel-zip-compress-pool", poolSize).create();
    ParallelScatterZipCreator parallelScatterZipCreator = new ParallelScatterZipCreator(executor);
    ZipArchiveOutputStream zipArchiveOutputStream = new ZipArchiveOutputStream(outputStream);
    zipArchiveOutputStream.setUseZip64(Zip64Mode.Always);
    zipArchiveOutputStream.setLevel(compressionLevel);

    try {
      try (final Stream<Path> stream = Files.walk(dirPath)) {
        for (Path subPath : stream.collect(toList())) {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }

          File file = subPath.toFile();

          final InputStreamSupplier inputStreamSupplier = () -> {
            try {
              if (file.exists() && file.isFile()) {
                return new FileInputStream(file);
              } else {
                return new NullInputStream(0);
              }
            } catch (FileNotFoundException e) {
              LOG.warn("Can't find file when parallel zip, path = {}", subPath);
              return new NullInputStream(0);
            }
          };

          String entryName = dirPath.relativize(subPath).toString();
          if (file.isDirectory()) {
            entryName += File.separator;
          }

          ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(entryName);
          zipArchiveEntry.setMethod(ZipArchiveEntry.DEFLATED);

          parallelScatterZipCreator.addArchiveEntry(zipArchiveEntry, inputStreamSupplier);
        }
      }

      parallelScatterZipCreator.writeTo(zipArchiveOutputStream);
      zipArchiveOutputStream.finish();
      zipArchiveOutputStream.flush();
    } catch (ExecutionException e) {
      LOG.error("Parallel compress rocksdb failed", e);
      throw new IOException(e);
    } finally {
      if (!executor.isTerminated()) {
        LOG.info("ParallelScatterZipCreator failed to shut down the thread pool, cleaning up now.");
        ExecutorServiceUtils.shutdownAndAwaitTermination(executor);
      }
    }

    LOG.info("Completed parallel compression for path {}, statistics: {}",
        dirPath, parallelScatterZipCreator.getStatisticsMessage().toString());
  }

  /**
   * Reads a zipped archive from a path in parallel and writes it to the given path.
   *
   * @param dirPath
   * @param backupPath
   * @param poolSize
   */
  public static void decompress(Path dirPath, String backupPath, int poolSize) throws IOException {
    LOG.info("decompress in parallel from path {} to {}", backupPath, dirPath);
    ExecutorService executor = ExecutorServiceFactories.fixedThreadPool(
        "parallel-zip-decompress-pool", poolSize).create();
    CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executor);

    try (ZipFile zipFile = new ZipFile(backupPath)) {
      Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
      int taskCount = 0;

      while (entries.hasMoreElements()) {
        taskCount += 1;
        ZipArchiveEntry entry = entries.nextElement();
        completionService.submit(() -> {
          unzipEntry(zipFile, entry, dirPath);
          return true;
        });
      }

      for (int i = 0; i < taskCount; i++) {
        completionService.take().get();
      }
    } catch (ExecutionException e) {
      LOG.error("Parallel decompress rocksdb fail", e);
      FileUtils.deletePathRecursively(dirPath.toString());
      throw new IOException(e);
    } catch (InterruptedException e) {
      LOG.info("Parallel decompress rocksdb interrupted");
      Thread.currentThread().interrupt();
      FileUtils.deletePathRecursively(dirPath.toString());
      throw new RuntimeException(e);
    } finally {
      ExecutorServiceUtils.shutdownAndAwaitTermination(executor);
    }
  }

  /**
   * Unzip entry in ZipFile.
   *
   * @param zipFile
   * @param entry
   * @param dirPath
   * @throws Exception
   */
  private static void unzipEntry(ZipFile zipFile, ZipArchiveEntry entry, Path dirPath)
      throws Exception {
    File outputFile = new File(dirPath.toFile(), entry.getName());
    outputFile.getParentFile().mkdirs();

    if (entry.isDirectory()) {
      outputFile.mkdir();
    } else {
      try (InputStream inputStream = zipFile.getInputStream(entry);
           FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
        IOUtils.copy(inputStream, fileOutputStream);
      }
    }
  }

  private ParallelZipUtils() {} // Utils class
}
