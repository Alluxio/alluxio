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

package alluxio.testutils;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread which performs operations on an Alluxio filesystem and tracks the results of the
 * operations.
 */
public class AlluxioOperationThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioOperationThread.class);

  private final FileSystem mFs;

  // Configurable state
  private List<Op> mOps = Arrays.asList(Op.values());
  private long mIntervalMs = 10;

  // Internal bookkeeping state
  private final AtomicReference<Throwable> mLatestFailure = new AtomicReference<>();
  private volatile boolean mStarted = false;
  private int mSuccesses = 0;

  /** Enum representing operations to perform on a cluster. */
  enum Op {
    /** Creates a small randomly named file at the root. */
    CREATE_FILE,
    /** Creates and deletes a file at the root. */
    CREATE_AND_DELETE;
  }

  /**
   * Constructs an {@link AlluxioOperationThread} which performs all types of operations.
   *
   * @param fs the filesystem on which to perform operations
   */
  public AlluxioOperationThread(FileSystem fs) {
    mFs = fs;
  }

  /**
   * @param ops the types of operation to perform
   * @return the updated operation thread object
   */
  public AlluxioOperationThread withOps(List<Op> ops) {
    Preconditions.checkState(ops.size() > 0);
    Preconditions.checkState(!mStarted);
    mOps = ops;
    return this;
  }

  /**
   * @param intervalMs the interval to wait between performing operations
   * @return the updated operation thread object
   */
  public AlluxioOperationThread withDelay(long intervalMs) {
    Preconditions.checkState(!mStarted);
    mIntervalMs = intervalMs;
    return this;
  }

  /**
   * @return the latest exception thrown during an operation, or null if no failures have occurred
   */
  public Throwable getLatestFailure() {
    return mLatestFailure.get();
  }

  /**
   * @return the number of successful operations performed by the thread
   */
  public int successes() {
    return mSuccesses;
  }

  @Override
  public void run() {
    mStarted = true;
    while (!Thread.interrupted()) {
      Op op = mOps.get(ThreadLocalRandom.current().nextInt(mOps.size()));
      try {
        switch (op) {
          case CREATE_FILE:
            createFile();
            break;
          case CREATE_AND_DELETE:
            mFs.delete(createFile());
            break;
          default:
            throw new IllegalStateException("Unknown op: " + op.toString());
        }
        mSuccesses++;
      } catch (Throwable t) {
        LOG.error("Failure during operation {}", op, t);
        mLatestFailure.set(t);
      }
      CommonUtils.sleepMs(mIntervalMs);
    }
  }

  private AlluxioURI createFile() {
    String file = "/file" + ThreadLocalRandom.current().nextLong();
    FileSystemTestUtils.createByteFile(mFs, file, 100, CreateFilePOptions.getDefaultInstance());
    return new AlluxioURI(file);
  }
}
