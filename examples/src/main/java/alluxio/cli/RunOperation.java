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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for running an operation multiple times.
 */
public class RunOperation {
  private static final String BASE_DIRECTORY = "/RunOperationDir";

  enum Operation {
    CreateEmptyFile,
    CreateAndDeleteEmptyFile;
  }

  @Parameter(names = {"-op", "-operation"},
      description = "the operation to perform. Options are [CreateEmptyFile, "
          + "CreateAndDeleteEmptyFile]",
      required = true)
  private Operation mOperation;
  @Parameter(names = {"-n", "-num"},
      description = "the number of times to perform the operation (total for all threads)")
  private int mTimes = 1;
  @Parameter(names = {"-t", "-threads"}, description = "the number of threads to use")
  private int mThreads = 1;

  private final FileSystem mFileSystem;

  /** Remaining number of times that the operation should be performed. */
  private AtomicInteger mRemainingOps;

  /**
   * Tool for running an operation multiple times.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    System.exit(new RunOperation().run(args));
  }

  /**
   * Constructs a new {@link RunOperation} object.
   */
  public RunOperation() {
    mFileSystem = FileSystem.Factory.get();
  }

  /**
   * @param args command line arguments
   * @return the exit status
   */
  public int run(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName("runOperation");
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.toString());
      System.out.println();
      jc.usage();
      return -1;
    }
    mRemainingOps = new AtomicInteger(mTimes);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < mThreads; i++) {
      threads.add(new OperationThread());
    }
    long start = System.currentTimeMillis();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        return -1;
      }
    }
    System.out.println("Completed in " + (System.currentTimeMillis() - start) + "ms");
    return 0;
  }

  private final class OperationThread extends Thread {
    private OperationThread() {}

    @Override
    public void run() {
      while (mRemainingOps.decrementAndGet() >= 0) {
        try {
          applyOperation();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void applyOperation() throws IOException, AlluxioException {
      AlluxioURI uri = new AlluxioURI(String.format("%s/%s", BASE_DIRECTORY, UUID.randomUUID()));
      switch (mOperation) {
        case CreateEmptyFile:
          mFileSystem.createFile(uri).close();
          break;
        case CreateAndDeleteEmptyFile:
          mFileSystem.createFile(uri).close();
          mFileSystem.delete(uri);
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + mOperation);
      }
    }
  }
}
