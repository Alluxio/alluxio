package alluxio.cli.fuse;

import static alluxio.cli.fuse.CorrectnessValidationUtils.BUFFER_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.DATA_INCONSISTENCY_FORMAT;
import static alluxio.cli.fuse.CorrectnessValidationUtils.DEFAULT_BUFFER_SIZE;
import static alluxio.cli.fuse.CorrectnessValidationUtils.FILE_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.TESTING_FILE_SIZE_FORMAT;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class validates the write correctness of AlluxioFuse.
 */
public class ValidateWrite {
  private static final String SEQUENTIAL_WRITE = "sequential write";

  /**
   * This method is the entry point for validating write correctness of AlluxioFuse.
   * @param options contains the options for the test
   */
  public static void validateWriteCorrectness(CorrectnessValidationOptions options) {
    for (long fileSize : FILE_SIZES) {
      // Thread and file is one-to-one in writing test
      List<Thread> threads = new ArrayList<>(options.getNumFiles());
      for (int i = 0; i < options.getNumFiles(); i++) {
        final int threadId = i;
        Thread t = new Thread(() -> {
          System.out.println(String.format(TESTING_FILE_SIZE_FORMAT, SEQUENTIAL_WRITE, fileSize));
          String localFilePath = CorrectnessValidationUtils
              .createLocalFile(fileSize, options.getLocalDir(), threadId);
          String fuseFilePath = "";
          for (int bufferSize : BUFFER_SIZES) {
            try {
              fuseFilePath = writeLocalFileToFuseMountPoint(
                  localFilePath, options.getFuseDir(), bufferSize);
              if (!validateData(localFilePath, fuseFilePath)) {
                System.out.println(String.format(
                    DATA_INCONSISTENCY_FORMAT, SEQUENTIAL_WRITE, bufferSize));
              }
            } catch (IOException e) {
              System.out.println(String.format(
                  "Error writing local file to fuse with file size %d and buffer size %d. %s",
                  fileSize, bufferSize, e));
            }
          }
          CorrectnessValidationUtils.deleteTestFiles(localFilePath, fuseFilePath);
        });
        threads.add(t);
      }
      for (Thread t: threads) {
        t.start();
        try {
          t.join();
        } catch (InterruptedException e) {
          System.out.println("Main thread is interrupted. Test is stopped");
          System.exit(1);
        }
      }
    }
  }

  private static String writeLocalFileToFuseMountPoint(
      String localFilePath, String fuseFileDirPath, int bufferSize) throws IOException {
    File dir = new File(fuseFileDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String fuseFilePath = Paths.get(fuseFileDirPath, Long.toString(System.currentTimeMillis()))
        .toString();
    try (FileInputStream localFileInputStream = new FileInputStream(localFilePath);
         FileOutputStream fuseFileOutputStream = new FileOutputStream(fuseFilePath, true)) {
      byte[] buffer = new byte[bufferSize];
      int bytesRead;
      while (true) {
        bytesRead = localFileInputStream.read(buffer);
        if (bytesRead == -1) {
          break;
        }
        fuseFileOutputStream.write(buffer, 0, bytesRead);
      }
    }
    return fuseFilePath;
  }

  private static boolean validateData(String localFilePath, String fuseFilePath) {
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
          return false;
        }
      }
    } catch (IOException e) {
      System.out.println(
          "Failed to create FileInputStream for validating data. Test is stopped. " + e);
      System.exit(1);
    }
    return true;
  }
}
