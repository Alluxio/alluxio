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

import alluxio.util.io.FileUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FileOutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Units tests for {@link ParallelZipUtils}.
 */
public final class ParallelZipUtilsTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void emptyDir() throws Exception {
    Path empty = mFolder.newFolder("emptyDir").toPath();

    zipUnzipTest(empty);
  }

  @Test
  public void oneFileDir() throws Exception {
    Path dir = mFolder.newFolder("oneFileDir").toPath();
    Path file = dir.resolve("file");
    Files.write(file, "test content".getBytes());

    zipUnzipTest(dir);
  }

  @Test
  public void tenFileDir() throws Exception {
    Path dir = mFolder.newFolder("tenFileDir").toPath();
    for (int i = 0; i < 10; i++) {
      Path file = dir.resolve("file" + i);
      Files.write(file, ("test content and a lot of test content" + i).getBytes());
    }

    zipUnzipTest(dir);
  }

  @Test
  public void emptySubDir() throws Exception {
    Path dir = mFolder.newFolder("emptySubDir").toPath();
    Path subDir = dir.resolve("subDir");
    Files.createDirectory(subDir);

    zipUnzipTest(dir);
  }

  @Test
  public void nested() throws Exception {
    Path dir = mFolder.newFolder("emptySubDir").toPath();
    Path current = dir;
    for (int i = 0; i < 10; i++) {
      Path newDir = current.resolve("dir" + i);
      Files.createDirectory(newDir);
      current = newDir;
    }
    Path file = current.resolve("file");
    Files.write(file, "hello world".getBytes());

    zipUnzipTest(dir);
  }

  @Test
  public void compressionTest() throws Exception {
    final String toCompress = "Some string that should be compressed."
        + "AbAAbAAAAbAAAAAAAAbAAAAAAAAAAAAAAAAAA";
    Path dir = mFolder.newFolder("emptySubDir").toPath();
    Path file = dir.resolve("file");
    Files.write(file, toCompress.getBytes());
    long nonCompressedSize = 0;
    long maxCompressedSize = 0;

    for (int compressionLevel = 0; compressionLevel < 10; compressionLevel++) {
      String zippedPath = mFolder.newFile("zipped").getPath();
      try (FileOutputStream fos = new FileOutputStream(zippedPath)) {
        ParallelZipUtils.compress(dir, fos, 5, compressionLevel);
      }
      if (compressionLevel == 0) {
        nonCompressedSize = Files.size(FileSystems.getDefault().getPath(zippedPath));
      } else {
        maxCompressedSize = Files.size(FileSystems.getDefault().getPath(zippedPath));
      }
      Path reconstructed = mFolder.newFolder("unzipped").toPath();
      reconstructed.toFile().delete();
      ParallelZipUtils.decompress(reconstructed, zippedPath, 5);
      FileUtil.assertDirectoriesEqual(dir, reconstructed);
      FileUtils.deletePathRecursively(reconstructed.toString());
      FileUtils.delete(zippedPath);
    }
    Assert.assertTrue(nonCompressedSize > maxCompressedSize);
  }

  private void zipUnzipTest(Path path) throws Exception {
    String zippedPath = mFolder.newFile("zipped").getPath();
    try (FileOutputStream fos = new FileOutputStream(zippedPath)) {
      ParallelZipUtils.compress(path, fos, 5, -1);
    }

    Path reconstructed = mFolder.newFolder("unzipped").toPath();
    reconstructed.toFile().delete();
    ParallelZipUtils.decompress(reconstructed, zippedPath, 5);
    FileUtil.assertDirectoriesEqual(path, reconstructed);
  }
}
