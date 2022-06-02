package alluxio.cli.fuse;

import static alluxio.cli.fuse.CorrectnessValidationUtils.FILE_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.BUFFER_SIZES;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ValidateRead {
  public static void validateReadCorrectness(CorrectnessValidationOptions options) {
    for (long size: FILE_SIZES) {
      String localFilePath = CorrectnessValidationUtils
          .createLocalFile(size, options.getLocalDir());
      String fuseFilePath = copyLocalFileToFuseMountPoint(localFilePath, options.getFuseDir());
      validateSequentialReadCorrectness(localFilePath, fuseFilePath, options.getNumThreads());
      validateRandomReadCorrectness(localFilePath, fuseFilePath, options.getNumThreads());
      CorrectnessValidationUtils.deleteTestFiles(localFilePath, fuseFilePath);
    }
  }

  private static String copyLocalFileToFuseMountPoint(String srcFilePath, String destDirPath) {
    String fuseFilePath = Paths.get(destDirPath, Long.toString(System.currentTimeMillis()))
        .toString();
    try {
      Files.copy(Paths.get(srcFilePath), Paths.get(fuseFilePath));
    } catch (IOException e) {
      System.out.println("Failed to copy local test file into Alluxio. Test is stopped.");
      System.exit(1);
    }
    return fuseFilePath;
  }

  private static void validateSequentialReadCorrectness(String localFilePath, String fuseFilePath, int numThreads) {
    for (int bufferSize: BUFFER_SIZES) {
      List<Thread> threads = new ArrayList<>(numThreads);
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        Thread t = new Thread(() -> {
          try (FileInputStream localInputStream = new FileInputStream(localFilePath);
              FileInputStream fuseInputStream = new FileInputStream(fuseFilePath)) {
            final byte[] localFileBuffer = new byte[bufferSize];
            final byte[] fuseFileBuffer = new byte[bufferSize];
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
            System.out.println(String.format("Thread %d IOException", threadId));
            System.out.println(e);
          }
        });
        threads.add(t);
      }
      for (Thread t: threads) {
        t.start();
        try {
          t.join();
        } catch (InterruptedException e) {
          System.out.println("Thread is interrupted. Test is stopped");
          System.exit(1);
        }
      }
    }
  }

  private static void validateRandomReadCorrectness(
      String localFilePath, String fuseFilePath, int numThreads) {
    for (int bufferSize: BUFFER_SIZES) {
      List<Thread> threads = new ArrayList<>(numThreads);
      for (int i = 0; i < numThreads; i++) {
        Thread t = new Thread(() -> {
          try (RandomAccessFile localRandomFile = new RandomAccessFile(localFilePath, "r");
               RandomAccessFile fuseRandomFile = new RandomAccessFile(fuseFilePath, "r")) {
            final byte[] localFileBuffer = new byte[bufferSize];
            final byte[] fuseFileBuffer = new byte[bufferSize];
            for (int iteration = 0; iteration < 50000; iteration++) {
              long offset = CorrectnessValidationUtils.nextRandomLong(localRandomFile.length());
              localRandomFile.seek(offset);
              fuseRandomFile.seek(offset);
              int localBytesRead = localRandomFile.read(localFileBuffer);
              int fuseBytesRead = localRandomFile.read(fuseFileBuffer);
              if (!CorrectnessValidationUtils.isDataCorrect(
                  localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
                System.out.println("Test failed. Data inconsistency found.");
                System.exit(1);
              }
            }
          } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
          }
        });
        threads.add(t);
      }
      for (Thread t: threads) {
        t.start();
        try {
          t.join();
        } catch (InterruptedException e) {
          System.out.println("Thread is interrupted. Test is stopped");
          System.exit(1);
        }
      }
    }
  }
}
