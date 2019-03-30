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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Profiles operations.
 */
public abstract class ProfilerClient {

  protected static final int CHUNK_SIZE = 10 * Constants.MB;
  protected static final byte[] DATA = new byte[CHUNK_SIZE];
  public static boolean sDryRun = false;

  /**
   * Create new.
   */
  public static class Factory {

    private static final AlluxioConfiguration CONF =
        new InstancedConfiguration(ConfigurationUtils.defaults());

    /**
     * Profiler client.
     * @param type alluxio or hadoop
     * @return profiler client
     */
    public static ProfilerClient create(String type) {
      switch (type) {
        case "abstractfs":
          Configuration conf = new Configuration();
          conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
          return new HadoopProfilerClient("alluxio:///", conf);
        case "alluxio":
          return new AlluxioProfilerClient();
        case "hadoop":
          return new HadoopProfilerClient(CONF.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS),
              new Configuration());
        default:
          throw new IllegalArgumentException(String.format("No profiler for %s", type));
      }
    }
  }

  protected void clientOperation(Runnable action, String msg) {
    if(!sDryRun) {
      action.run();
    } else {
      System.out.println(msg);
    }
  }

  static void writeOutput(OutputStream os, long fileSize) throws IOException {
    for (int k = 0; k < fileSize / CHUNK_SIZE; k++) {
      os.write(DATA);
    }
    os.write(Arrays.copyOf(DATA, (int)(fileSize % CHUNK_SIZE))); // Write any remaining data
  }

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @throws IOException
   */
  public final void cleanup(String dir) throws IOException {
    delete(dir);
    createDir(dir);
  }

  /**
   * Create a file with a given size at the path.
   *
   * @param rawPath directory to clean
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
   * Delete an inode at the given path
   *
   * @param rawPath directory to clean
   * @throws IOException
   */
  public abstract void delete(String rawPath) throws IOException;

  /**
   * list the status of an inode at the path
   *
   * @param rawPath path of the inode to list
   * @throws IOException
   */
  public abstract void list(String rawPath) throws IOException;

  /**
   * Lists the statuses files underneath the given dir in a directory structure determined by
   * the number of files, threads, and files per directory.
   *
   * If numFiles doesn't divide evenly into numThreads, the remainder number of files is truncated.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @throws IOException
   */
  public final void listFiles(String dir, long numFiles, long filesPerDir, int numThreads,
      int ratePerSecond)
      throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (dirName) -> {},
        (fileName) -> {
      try {
        list(fileName);
      } catch (IOException e) { }
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
   * @throws IOException
   */
  public final void createFiles(String dir, long numFiles, long filesPerDir, long fileSize,
      int numThreads, int ratePerSecond) throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (dirName) -> {
      try {
        createDir(dirName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }},
        (fileName) -> {
      try {
        createFile(fileName, fileSize);
      } catch (IOException e) { }
        });
  }

  /**
   * Creates files underneath the given dir in a directory structure determined by the
   *
   * If numFiles doesn't divide evenly into numThreads, the remainder number of files is truncated.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @throws IOException
   */
  public final void deleteFiles(String dir, long numFiles, long filesPerDir, int numThreads,
      int ratePerSecond)
      throws IOException {
    runOperation(dir, numFiles, filesPerDir, numThreads, ratePerSecond,
        (deleteDir) -> {},
        (deleteFile) ->  {
      try {
        delete(deleteFile);
      } catch (IOException e) { }
    });
  }

  /**
   * Given a directory, will execute the provided operations when calculating each directory
   * name and each file name. The number of operations on each file is rate limited to
   * {@code ratePerSecond} operations per second.
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
    for (int i = 0; i < numThreads; i++) {
      int threadNum = i; //
      // number of operations to perform on each thread
      int numThreadFiles = (int)numFiles / numThreads;
      threads.add(new Thread(() -> {
        String threadDir = PathUtils.concatPath(dir, String.format("profilerThread-%d", threadNum));
        int createdFiles = 0;
        while (createdFiles < numThreadFiles) {
          String subDir = PathUtils.concatPath(threadDir, createdFiles);
          dirRunnable.accept(subDir);
          for (int j = 0; j < filesPerDir && createdFiles < numThreadFiles; j++) {
            String filePath = PathUtils.concatPath(subDir, j);
            limiter.acquire();
            fileOpRunnable.accept(filePath);
            createdFiles++;
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
