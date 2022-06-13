package alluxio.cli.fuse;

import static alluxio.cli.fuse.CorrectnessValidationUtils.BUFFER_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.DATA_INCONSISTENCY_FORMAT;
import static alluxio.cli.fuse.CorrectnessValidationUtils.FILE_SIZES;
import static alluxio.cli.fuse.CorrectnessValidationUtils.TESTING_FILE_SIZE_FORMAT;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class validates the read correctness of AlluxioFuse.
 */
public class ValidateRead {
  private static final String READ = "read";
  private static final String RANDOM_READ = "random read";
  private static final String SEQUENTIAL_READ = "sequential read";

  /**
   * This method is the entry point for validating read correctness of AlluxioFuse.
   * @param options contains the options for the test
   */
  public static void validateReadCorrectness(CorrectnessValidationOptions options) {
    for (long fileSize: FILE_SIZES) {
      System.out.println(String.format(TESTING_FILE_SIZE_FORMAT, READ, fileSize));
      if (options.getNumFiles() == 1) {
        String localFilePath = CorrectnessValidationUtils
            .createLocalFile(fileSize, options.getLocalDir(), 0);
        String fuseFilePath = copyLocalFileToFuseMountPoint(localFilePath, options.getFuseDir());
        for (int bufferSize: BUFFER_SIZES) {
          validateSequentialReadCorrectness(
              localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          validateRandomReadCorrectness(
              localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          if (options.getNumThreads() > 1) {
            validateMixedReadCorrectness(
                localFilePath, fuseFilePath, options.getNumThreads(), bufferSize);
          }
        }
        CorrectnessValidationUtils.deleteTestFiles(localFilePath, fuseFilePath);
      } else {
        // multiple files
      }
    }
  }

  private static String copyLocalFileToFuseMountPoint(String srcFilePath, String destDirPath) {
    Path fuseFilePath = Paths.get(destDirPath, Long.toString(System.currentTimeMillis()));
    try {
      Files.copy(Paths.get(srcFilePath), fuseFilePath);
    } catch (IOException e) {
      System.out.println("Failed to copy local test file into Alluxio. Test is stopped.");
      System.exit(1);
    }
    return fuseFilePath.toString();
  }

  private static void validateSequentialReadCorrectness(
      String localFilePath, String fuseFilePath, int numThreads, int bufferSize) {
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
              System.out.println(String.format(
                  DATA_INCONSISTENCY_FORMAT, SEQUENTIAL_READ, bufferSize));
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
        System.out.println("Main thread is interrupted. Test is stopped");
        System.exit(1);
      }
    }
  }

  private static void validateRandomReadCorrectness(
      String localFilePath, String fuseFilePath, int numThreads, int bufferSize) {
    List<Thread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
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
            int fuseBytesRead = fuseRandomFile.read(fuseFileBuffer);
            if (!CorrectnessValidationUtils.isDataCorrect(
                localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
              System.out.println(String.format(
                  DATA_INCONSISTENCY_FORMAT, RANDOM_READ, bufferSize));
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
        System.out.println("Main thread is interrupted. Test is stopped");
        System.exit(1);
      }
    }
  }

  // Half of all threads do sequential read and the other half do random read.
  private static void validateMixedReadCorrectness(
      String localFilePath, String fuseFilePath, int numThreads, int bufferSize) {
    Thread sequentialRead = new Thread(() -> {
      validateSequentialReadCorrectness(localFilePath, fuseFilePath, numThreads / 2, bufferSize);
    });
    Thread randomRead = new Thread(() -> {
      validateRandomReadCorrectness(
          localFilePath, fuseFilePath, numThreads / 2 + numThreads % 2, bufferSize);
    });
    sequentialRead.start();
    randomRead.start();
    try {
      sequentialRead.join();
      randomRead.join();
    } catch (InterruptedException e) {
      System.out.println("Main thread is interrupted. Test is stopped");
      System.exit(1);
    }
  }
}
