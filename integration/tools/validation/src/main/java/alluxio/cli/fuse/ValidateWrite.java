package alluxio.cli.fuse;

import static alluxio.cli.fuse.CorrectnessValidationUtils.DEFAULT_BUFFER_SIZE;
import static alluxio.cli.fuse.CorrectnessValidationUtils.BUFFER_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.FILE_SIZES;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * This class validates the write correctness of AlluxioFuse.
 */
public class ValidateWrite {

  /**
   * This method is the entry point for validating write correctness of AlluxioFuse
   * @param options contains the options for the test
   */
  public static void validateWriteCorrectness(CorrectnessValidationOptions options) {
    for (long size : FILE_SIZES) {
      String localFilePath = CorrectnessValidationUtils
          .createLocalFile(size, options.getLocalDir());
      for (int bufferSize : BUFFER_SIZES) {
        String fuseFilePath = writeLocalFileToFuseMountPoint(
            localFilePath, options.getFuseDir(), bufferSize);
        validateData(localFilePath, fuseFilePath);
        CorrectnessValidationUtils.deleteTestFiles(localFilePath, fuseFilePath);
      }
    }
  }

  private static String writeLocalFileToFuseMountPoint(
      String localFilePath, String fuseFileDirPath, int bufferSize) {
    File dir = new File(fuseFileDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String fuseFilePath = Paths.get(fuseFileDirPath, Long.toString(System.currentTimeMillis()))
        .toString();
    try (FileInputStream localFileInputStream = new FileInputStream(localFilePath);
         FileOutputStream fuseFileOutputStream = new FileOutputStream(fuseFilePath)) {
      byte[] buffer = new byte[bufferSize];
      int bytesRead = 0;
      while (true) {
        bytesRead = localFileInputStream.read(buffer);
        if (bytesRead == -1) {
          break;
        }
        fuseFileOutputStream.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      System.out.println("Failed to create local test file. Test is stopped. " + e);
      System.exit(1);
    }
    return fuseFilePath;
  }

  private static void validateData(String localFilePath, String fuseFilePath) {
    try (FileInputStream localInputStream = new FileInputStream(localFilePath);
         FileInputStream fuseInputStream = new FileInputStream(fuseFilePath)) {
      final byte[] localFileBuffer = new byte[DEFAULT_BUFFER_SIZE];
      final byte[] fuseFileBuffer = new byte[DEFAULT_BUFFER_SIZE];
      int localBytesRead = 0;
      int fuseBytesRead = 0;
      while (localBytesRead != -1 || fuseBytesRead != -1) {
        localBytesRead = localInputStream.read(localFileBuffer);
        fuseBytesRead = fuseInputStream.read(fuseFileBuffer);
        if (!CorrectnessValidationUtils.isDataCorrect(
            localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
          System.out.println("Test failed. Data inconsistency found.");
          System.exit(1);
        }
      }
    } catch (IOException e) {
      System.out.println("Failed to create FileInputStream for validating data. Test is stopped. " + e);
      System.exit(1);
    }
  }
}
