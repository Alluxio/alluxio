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

package alluxio.cli.bundler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TarUtilsTest {
  @Test
  public void compressAndDecompressFiles() throws IOException {
    // create temp dir
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();

    int fileCount = 10;
    // create a list of files in the folder
    for (int i = 0; i < fileCount; i++) {
      File subFile = new File(targetDir, "file_" + i);
      subFile.createNewFile();
    }
    createFilesInDir(targetDir, 10);
    File[] filesToCompress = targetDir.listFiles();

    // Compress the file
    String tarballName = "tarball.tar.gz";
    String gzFilePath = new File(targetDir, tarballName).getAbsolutePath();
    TarUtils.compress(gzFilePath, filesToCompress);
    assertTrue(new File(gzFilePath).exists());

    // Decompress the compressed file
    File outputDir = new File(targetDir, "recovered");
    outputDir.mkdir();
    TarUtils.decompress(gzFilePath, outputDir);

    // Verify the recovered files
    compareFiles(filesToCompress, outputDir.listFiles());
  }

  @Test
  public void compressAndDecompressFilesAndFolder() throws IOException {
    // create temp dir
    File source1 = InfoCollectorTestUtils.createTemporaryDirectory();
    File source2 = InfoCollectorTestUtils.createTemporaryDirectory();
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();

    createFilesInDir(source1, 10);
    createFilesInDir(source2, 10);
    File[] filesToCompress = new File[]{source1, source2};

    // Compress the file
    String tarballName = "tarball.tar.gz";
    String gzFilePath = new File(targetDir, tarballName).getAbsolutePath();
    TarUtils.compress(gzFilePath, filesToCompress);
    assertTrue(new File(gzFilePath).exists());

    // Decompress the compressed file
    File outputDir = new File(targetDir, "recovered");
    outputDir.mkdir();
    TarUtils.decompress(gzFilePath, outputDir);

    // Verify the recovered files
    compareFiles(filesToCompress, outputDir.listFiles());
  }

  private static void createFilesInDir(File dir, int numOfFiles) throws IOException {
    int fileCount = numOfFiles;
    // create a list of files
    for (int i = 0; i < fileCount; i++) {
      File subFile = new File(dir, "file_" + i);
      subFile.createNewFile();
    }
  }

  private static void compareFiles(File[] expected, File[] recovered) {
    assertEquals(expected.length, recovered.length);

    Arrays.sort(expected);
    Arrays.sort(recovered);
    for (int i = 0; i < expected.length; i++) {
      assertTrue(recovered[i].exists());
      if (expected[i].isDirectory()) {
        assertTrue(recovered[i].isDirectory());
        // recursively compare children
        compareFiles(expected[i].listFiles(), recovered[i].listFiles());
      } else {
        // compare two files
        assertEquals(expected[i].getName(), recovered[i].getName());
        assertEquals(expected[i].length(), recovered[i].length());
      }
    }
  }
}
