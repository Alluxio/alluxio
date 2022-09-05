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

import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Utility methods for working with parallel zip archives.
 */
public class ParallelZipUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelZipUtils.class);

  /**
   * Creates a zipped archive from the given path in parallel, streaming the data
   * to the give output path.
   *
   * @param dirPath
   * @param backupPath
   * @param poolSize
   * @throws IOException
   * @throws InterruptedException
   */
  public static void compress(Path dirPath, String backupPath, int poolSize)
      throws IOException, InterruptedException {
    LOG.info("compress in parallel from path " + dirPath + " to " + backupPath);
    ExecutorService executor = ExecutorServiceFactories.fixedThreadPool(
        "parallel-zip-compress-pool", poolSize).create();
    ParallelScatterZipCreator parallelScatterZipCreator = new ParallelScatterZipCreator(executor);

    try (FileOutputStream fileOutputStream = new FileOutputStream(backupPath);
         BufferedOutputStream bufferedOutputStream =
             new BufferedOutputStream(fileOutputStream);
         ZipArchiveOutputStream zipArchiveOutputStream =
             new ZipArchiveOutputStream(bufferedOutputStream)) {
      try (final Stream<Path> stream = Files.walk(dirPath)) {
        for (Path subPath : stream.collect(toList())) {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }

          if (subPath.equals(dirPath)) {
            continue;
          }

          File file = subPath.toFile();

          final InputStreamSupplier inputStreamSupplier = () -> {
            try {
              return new FileInputStream(file);
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
      zipArchiveOutputStream.flush();
      fileOutputStream.getFD().sync();
    } catch (ExecutionException e) {
      LOG.error("Parallel compress rocksdb fail", e);
      throw new RuntimeException(e);
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdownNow();
      }
    }

    LOG.info("compress in parallel from path " + dirPath + " to " + backupPath
        + " statistics message "
        + parallelScatterZipCreator.getStatisticsMessage().toString());
  }

  /**
   * Reads a zipped archive from a path in parallel and writes it to the given path.
   *
   * @param dirPath
   * @param backupPath
   * @param poolSize
   */
  public static void deCompress(Path dirPath, String backupPath, int poolSize) {
    LOG.info("deCompress in parallel from path " + backupPath + " to " + dirPath);
    ExecutorService executor = ExecutorServiceFactories.fixedThreadPool(
        "parallel-zip-deCompress-pool", poolSize).create();

    dirPath.toFile().mkdirs();

    try (ZipFile zipFile = new ZipFile(backupPath)) {
      Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
      List<Future<Boolean>> futures = new LinkedList<>();

      while (entries.hasMoreElements()) {
        ZipArchiveEntry entry = entries.nextElement();
        Future<Boolean> future = executor.submit(() -> {
          unzipEntry(zipFile, entry, dirPath);
          return true;
        });

        futures.add(future);
      }

      for (Future<Boolean> future : futures) {
        future.get();
      }

      ExecutorServiceUtils.shutdownAndAwaitTermination(executor);
    } catch (Exception e) {
      LOG.error("Parallel deCompress rocksdb fail", e);
      throw new RuntimeException(e);
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdownNow();
      }
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
