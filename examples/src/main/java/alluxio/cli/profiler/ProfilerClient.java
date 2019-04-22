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

package alluxio.cli.profiler;

import alluxio.Constants;
import alluxio.cli.ClientProfiler;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * A client which can be used to profile common filesystem operations.
 */
public abstract class ProfilerClient {

  private static final int CHUNK_SIZE = 10 * Constants.MB;
  private static final byte[] WRITE_BUFFER = new byte[CHUNK_SIZE];
  private static final byte[] READ_BUFFER = new byte[CHUNK_SIZE];

  /**
   * Whether or not all operations should be executed as dry runs.
   *
   * A dry run will not execute any real operations and will only print the expected operations
   * to the terminal.
   */
  public static boolean sDryRun = false;

  /**
   * Create new.
   */
  public static class Factory {

    private static final AlluxioConfiguration CONF =
        new InstancedConfiguration(ConfigurationUtils.defaults());

    /**
     * Create profiler client based on the type.
     *
     * abtractfs will utilize alluxio's hadoop wrapper
     * alluxio is the native alluxio client
     * hadoop is the hadoop java client.
     *
     * @param type the type of client to profile
     * @return profiler client for the given type
     */
    public static ProfilerClient create(ClientProfiler.ClientType type) {
      switch (type) {
        case abstractfs:
          Configuration conf = new Configuration();
          conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
          return new HadoopProfilerClient("alluxio:///", conf);
        case alluxio:
          return new AlluxioProfilerClient();
        case hadoop:
          return new HadoopProfilerClient(CONF.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS),
              new Configuration());
        default:
          throw new IllegalArgumentException(String.format("No profiler for %s", type));
      }
    }
  }

  static void writeOutput(OutputStream os, long fileSize) throws IOException {
    for (int k = 0; k < fileSize / CHUNK_SIZE; k++) {
      os.write(WRITE_BUFFER);
    }
    os.write(Arrays.copyOf(WRITE_BUFFER, (int) (fileSize % CHUNK_SIZE))); // Write any remaining
    // data
  }

  static void readInput(InputStream is) throws IOException {
    while (is.read(READ_BUFFER) != -1) {
      continue;
    }
  }

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @throws IOException
   */
  public final void cleanup(String dir) throws IOException {
    if (!sDryRun) {
      delete(dir);
      createDir(dir);
    } else {
      System.out.println(String.format("recursive delete %s", dir));
      System.out.println(String.format("create dir %s", dir));
    }
  }

  /**
   * Create a file with a given size at the path.
   *
   * @param rawPath directory to clean
   * @param fileSize the amount of data to write to the file
   * @throws IOException
   */
  public abstract void createFile(String rawPath, long fileSize) throws IOException;

  /**
   * Create a directory at the given path.
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  public abstract void createDir(String rawPath) throws IOException;

  /**
   * Delete an inode at the given path.
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  public abstract void delete(String rawPath) throws IOException;

  /**
   * list the status of an inode at the path.
   *
   * @param rawPath path of the inode to list
   * @throws IOException
   */
  public abstract void list(String rawPath) throws IOException;

  /**
   * Reads through all of the bytes of a file at the given path.
   *
   * @param rawPath the path of the file in the filesystem
   * @throws IOException
   */
  public abstract void read(String rawPath) throws IOException;

  /**
   * Lists the statuses files underneath the given dir in a directory structure determined by
   * the number of files, threads, and files per directory.
   *
   * If numFiles doesn't divide evenly into numThreads, the remainder number of files is truncated.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param numThreads the number of threads to utilize
   * @param ratePerSecond the amount of lists allowed per second
   * @throws IOException
   */
  public final void listFiles(String dir, long numFiles, long filesPerDir, int numThreads,
      int ratePerSecond)
      throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (dirName) -> { },
        (fileName) -> {
          try {
            if (!sDryRun) {
              list(fileName);
            } else {
              System.out.println("list file: " + fileName);
            }
          } catch (IOException e) {
            // Ignore exception
          }
        });
  }

  /**
   * Lists the statuses files underneath the given dir in a directory structure determined by
   * the number of files, threads, and files per directory.
   *
   * If numFiles doesn't divide evenly into numThreads, the remainder number of files is truncated.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param fileSize the size in bytes of each file that will be created
   * @param numThreads the number of threads that allow concurrent operations
   * @param ratePerSecond the number of creates allowed per second
   * @throws IOException
   */
  public final void createFiles(String dir, long numFiles, long filesPerDir, long fileSize,
      int numThreads, int ratePerSecond) throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (dirName) -> {
          try {
            if (!sDryRun) {
              createDir(dirName);
            } else {
              System.out.println("create dir: " + dirName);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        (fileName) -> {
          try {
            if (!sDryRun) {
              createFile(fileName, fileSize);
            } else {
              System.out.println("create file: " + fileName);
            }
          } catch (IOException e) {
            // Ignore exception
          }
        });
  }

  /**
   * Creates files underneath the given dir in a directory structure determined by
   * the number of files, threads, and files per directory.
   *
   * If numFiles doesn't divide evenly into numThreads, the remainder number of files is truncated.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param numThreads the number of threads that allow concurrent operations
   * @param ratePerSecond the number of creates allowed per second
   * @throws IOException
   */
  public final void deleteFiles(String dir, long numFiles, long filesPerDir, int numThreads,
      int ratePerSecond) throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (deleteDir) -> { },
        (deleteFile) -> {
          try {
            if (!sDryRun) {
              delete(deleteFile);
            } else {
              System.out.println("delete: " + deleteFile);
            }
          } catch (IOException e) {
            // Ignore exception
          }
        });
  }

  /**
   * Reads files underneath the given dir in a directory structure determined by
   * the number of files, threads, and files per directory.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param numThreads the number of threads that allow concurrent operations
   * @param ratePerSecond the number of creates allowed per second
   * @throws IOException
   */
  public final void readFiles(String dir, long numFiles, long filesPerDir, int numThreads,
      int ratePerSecond) throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (readDir) -> { },
        (readFile) -> {
          try {
            if (!sDryRun) {
              read(readFile);
            } else {
              System.out.println("read file: " + readFile);
            }
          } catch (IOException e) {
            // Ignore exception
          }
        });
  }

  /**
   * Given a directory, will execute the provided operations when calculating each directory
   * name and each file name. The number of operations on each file is rate limited to
   * {@code ratePerSecond} operations per second.
   *
   * The directory structure mimics the following pattern:
   *
   * ---- ${dir}
   *         |
   *         |
   *         +-- profilerThread-0
   *         +-- profilerThread-1
   *         |    ...
   *         +-- profilerThread-${numThreads}
   *                |
   *                |
   *                +-- 0
   *                +-- 1
   *                |   ...
   *                +-- ${numFiles}/${filesPerDir}
   *                        |
   *                        |
   *                        +-- 0
   *                        +-- 1
   *                        |   ...
   *                        +-- ${filesPerDir}
   *
   *
   * @param dir The directory to run operations in
   * @param numFiles the number of files which is expected
   * @param filesPerDir the number of expected files within each directory
   * @param numThreads numThreads
   * @param ratePerSecond the number of operations to allow per second
   * @param dirRunnable the runnable which accepts a directory path and performs an operation
   *        using it
   * @param fileOpRunnable the runnable which accepts a file path and performs an operation using it
   */
  private static void runOperation(String dir, long numFiles, long filesPerDir,
      int numThreads,
      int ratePerSecond, Consumer<String> dirRunnable, Consumer<String> fileOpRunnable) {
    Preconditions.checkState(numFiles >= numThreads, "number of files must be > num threads");

    List<Thread> threads = new ArrayList<>();
    RateLimiter limiter = RateLimiter.create(ratePerSecond);
    int numThreadFiles = ((int) numFiles) / numThreads;
    for (int i = 0; i < numThreads; i++) {
      int threadNum = i;
      // number of operations to perform on each thread
      // purposefully truncating any remainder
      threads.add(new Thread(() -> {
        String threadDir = PathUtils.concatPath(dir, String.format("profilerThread-%d", threadNum));
        int fileOps = 0;
        while (fileOps < numThreadFiles) {
          String subDir = PathUtils.concatPath(threadDir, fileOps);
          dirRunnable.accept(subDir);
          for (int j = 0; j < filesPerDir && fileOps < numThreadFiles; j++) {
            String filePath = PathUtils.concatPath(subDir, j);
            limiter.acquire();
            fileOpRunnable.accept(filePath);
            fileOps++;
          }
        }
      }));
    }
    threads.forEach(Thread::start);
    waitForThreads(threads);
  }

  private static void waitForThreads(List<Thread> threads) {
    threads.forEach((thread) -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        // do nothing
      }
    });
  }
}
