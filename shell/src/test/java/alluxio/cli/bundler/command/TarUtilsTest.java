package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TarUtilsTest {
  @Test
  public void compressAndDecompressFiles() throws IOException {
    // create temp dir
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("logTarget");

    int fileCount = 10;
    // create a list of files in the folder
    for (int i=0; i<fileCount; i++) {
      File subFile = new File(targetDir, "file_"+i);
      subFile.createNewFile();
    }
    createFilesInDir(targetDir, 10);
    File[] filesToCompress = targetDir.listFiles();

    // Compress the file
    String tarballName = "tarball.tar.gz";
    String gzFilePath = Paths.get(targetDir.toPath().toString(), tarballName).toAbsolutePath().toString();
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
    File source1 = AlluxioTestDirectory.createTemporaryDirectory("source1");
    File source2 = AlluxioTestDirectory.createTemporaryDirectory("source2");
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("output");

    createFilesInDir(source1, 10);
    createFilesInDir(source2, 10);
    File[] filesToCompress = new File[]{source1, source2};

    // Compress the file
    String tarballName = "tarball.tar.gz";
    String gzFilePath = Paths.get(targetDir.toPath().toString(), tarballName).toAbsolutePath().toString();
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
    for (int i=0; i<fileCount; i++) {
      File subFile = new File(dir, "file_"+i);
      subFile.createNewFile();
    }
  }

  private static void compareFiles(File[] expected, File[] recovered) {
    assertEquals(expected.length, recovered.length);

    Arrays.sort(expected);
    Arrays.sort(recovered);
    for (int i=0; i<expected.length; i++) {
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
