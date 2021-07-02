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

package alluxio.client.fs.concurrent;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests to validate the concurrency in {@link FileSystemMaster}. These tests all use a local
 * path as the under storage system.
 *
 * The tests validate the correctness of concurrent operations, ie. no corrupted/partial state is
 * exposed, through a series of concurrent operations followed by verification of the final
 * state, or inspection of the in-progress state as the operations are carried out.
 *
 * The tests also validate that operations are concurrent by injecting a short sleep in the
 * critical code path. Tests will timeout if the critical section is performed serially.
 */
public class ConcurrentFileSystemMasterUtils {
  private static final String TEST_USER = "test";

  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the file
   * but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFilePOptions sCreatePersistedFileOptions = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.THROUGH).setRecursive(true).build();

  /**
   * Unary file operations for concurrent tests.
   */
  public enum UnaryOperation {
    CREATE,
    DELETE,
    GET_FILE_INFO,
    LIST_STATUS
  }

  /**
   * Uses the integer suffix of a path to determine order. Paths without integer suffixes will be
   * ordered last.
   */
  public static class IntegerSuffixedPathComparator implements Comparator<URIStatus> {
    @Override
    public int compare(URIStatus o1, URIStatus o2) {
      return extractIntegerSuffix(o1.getName()) - extractIntegerSuffix(o2.getName());
    }

    private int extractIntegerSuffix(String name) {
      Pattern p = Pattern.compile("\\D*(\\d+$)");
      Matcher m = p.matcher(name);
      if (m.matches()) {
        return Integer.parseInt(m.group(1));
      } else {
        return Integer.MAX_VALUE;
      }
    }
  }

  /**
   * Helper for running concurrent operations. Enforces that the run time of this method is not
   * greater than the specified run time.
   *
   * @param fileSystem the filesystem to use
   * @param operation the operation to run concurrently
   * @param paths the paths to run the operation on
   * @param limitMs the maximum allowable run time, in ms
   * @return all exceptions encountered
   */
  public static List<Throwable> unaryOperation(final FileSystem fileSystem,
      final UnaryOperation operation, final AlluxioURI[] paths, final long limitMs)
      throws Exception {
    final int numFiles = paths.length;
    final CyclicBarrier barrier = new CyclicBarrier(numFiles);
    List<Thread> threads = new ArrayList<>(numFiles);
    // If there are exceptions, we will store them here.
    final List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>());
    Thread.UncaughtExceptionHandler exceptionHandler = (th, ex) -> errors.add(ex);
    for (int i = 0; i < numFiles; i++) {
      final int iteration = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            switch (operation) {
              case CREATE:
                fileSystem.createFile(paths[iteration], sCreatePersistedFileOptions).close();
                break;
              case DELETE:
                fileSystem.delete(paths[iteration]);
                break;
              case GET_FILE_INFO:
                URIStatus status = fileSystem.getStatus(paths[iteration]);
                if (!status.isFolder()) {
                  Assert.assertNotEquals(0, status.getBlockIds().size());
                }
                break;
              case LIST_STATUS:
                fileSystem.listStatus(paths[iteration]);
                break;
              default: throw new IllegalArgumentException("'operation' is not a valid operation.");
            }
          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      });
      t.setUncaughtExceptionHandler(exceptionHandler);
      threads.add(t);
    }
    Collections.shuffle(threads);
    long startMs = CommonUtils.getCurrentMs();
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    long durationMs = CommonUtils.getCurrentMs() - startMs;
    Assert.assertTrue("Execution duration " + durationMs + " took longer than expected " + limitMs,
        durationMs < limitMs);
    return errors;
  }
}
