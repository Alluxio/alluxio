package alluxio.cli.fuse;

import static alluxio.Constants.GB;
import static alluxio.Constants.KB;
import static alluxio.Constants.MB;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

/**
 * This class contains utility functions for validating fuse correctness.
 */
public class CorrectnessValidationUtils {
  public static final long[] FILE_SIZES = {100 * KB, MB, 1059062, 63 * MB, 65 * MB, GB, 10L * GB};
  public static final int[] BUFFER_SIZES = {
      128, 1000, 1001, MB, 1025, 4 * KB, 32 * KB, 128 * KB, MB, 4 * MB};
  public static final int DEFAULT_BUFFER_SIZE = MB;
  public static final String TESTING_FILE_SIZE_FORMAT = "Starting testing %s of file size %d.";
  public static final String DATA_INCONSISTENCY_FORMAT =
      "Data inconsistency found while testing %s with buffer size %d.";
  private static final Random RANDOM = new Random();
  private static final ThreadLocalRandom THREAD_LOCAL_RANDOM = ThreadLocalRandom.current();

  /**
   * Creates a local file given size and directory.
   * @param size file size
   * @param dirPath the dir where the created file is
   * @param id an id number associated with the file being created
   * @return the path of the created file
   */
  public static String createLocalFile(long size, String dirPath, int id) {
    File dir = new File(dirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String localFilePath = String.format("%s-%d",
        Paths.get(dirPath, Long.toString(System.currentTimeMillis())), id);
    try (FileOutputStream outputStream = new FileOutputStream(localFilePath)) {
      int bufferSize = (int) Math.min(DEFAULT_BUFFER_SIZE, size);
      byte[] buf = new byte[bufferSize];
      while (size > 0) {
        RANDOM.nextBytes(buf);
        int sizeToWrite = (int) Math.min(bufferSize, size);
        outputStream.write(buf, 0, sizeToWrite);
        size -= sizeToWrite;
      }
    } catch (IOException e) {
      System.out.println("Failed to create local test file. Test is stopped.");
      System.exit(1);
    }
    return localFilePath;
  }

  /**
   * Checks if the two buffers contain the same data.
   * @param localFileBuffer the buffer contains the local file content
   * @param fuseFileBuffer the buffer contains the fuse file content
   * @param localBytesRead the valid number of bytes in the localFileBuffer
   * @param fuseBytesRead the valid number of bytes in the fuseBytesBuffer
   * @return true if they contain the same data; false otherwise
   */
  public static boolean isDataCorrect(
      byte[] localFileBuffer, byte[] fuseFileBuffer, int localBytesRead, int fuseBytesRead) {
    if (localBytesRead != fuseBytesRead) {
      return false;
    }
    for (int i = 0; i < localBytesRead; i++) {
      if (localFileBuffer[i] != fuseFileBuffer[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Deletes target files.
   * @param localFilePath the path of the local file
   * @param fuseFilePath the path of the fuse file
   */
  public static void deleteTestFiles(String localFilePath, String fuseFilePath) {
    try {
      Files.delete(Paths.get(localFilePath));
      Files.delete(Paths.get(fuseFilePath));
    } catch (IOException e) {
      System.out.println("Error deleting testing files. " + e);
    }
  }

  /**
   * Generates a long within the bound.
   * @param bound the upperbound of the random long
   * @return a random long number
   */
  public static long nextRandomLong(long bound) {
    return THREAD_LOCAL_RANDOM.nextLong(bound);
  }

  private CorrectnessValidationUtils() {}
}

