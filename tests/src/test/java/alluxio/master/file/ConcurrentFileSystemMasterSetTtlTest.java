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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.BaseIntegrationTest;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.collections.ConcurrentHashSet;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

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
public class ConcurrentFileSystemMasterSetTtlTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /** Timeout for the concurrent test after which we will mark the test as failed. */
  private static final long LIMIT_MS = SLEEP_MS * CONCURRENCY_FACTOR / 10;
  /** The interval of the ttl bucket. */
  private static final int TTL_INTERVAL_MS = 100;

  private FileSystem mFileSystem;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setProperty(PropertyKey
          .USER_FILE_MASTER_CLIENT_THREADS, CONCURRENCY_FACTOR)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, TTL_INTERVAL_MS).build();

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Test
  public void rootFileConcurrentSetTtlTest() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] files = new AlluxioURI[numThreads];
    long[] ttls = new long[numThreads];
    Random random = new Random();

    for (int i = 0; i < numThreads; i++) {
      files[i] = new AlluxioURI("/file" + i);
      mFileSystem.createFile(files[i],
          CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)).close();
      ttls[i] = random.nextInt(2 * TTL_INTERVAL_MS);
    }

    assertErrorsSizeEquals(concurrentSetTtl(files, ttls), 0);

    // Wait for all the created files being deleted after the TTLs become expired.
    CommonUtils.sleepMs(4 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);

    Assert.assertEquals("There're remaining file existing with expired TTLs",
        0, mFileSystem.listStatus(new AlluxioURI("/")).size());
  }

  private ConcurrentHashSet<Throwable> concurrentSetTtl(final AlluxioURI[] paths, final long[] ttls)
      throws Exception {
    final int numFiles = paths.length;
    final CyclicBarrier barrier = new CyclicBarrier(numFiles);
    List<Thread> threads = new ArrayList<>(numFiles);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        errors.add(ex);
      }
    };
    for (int i = 0; i < numFiles; i++) {
      final int iteration = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            mFileSystem.setAttribute(paths[iteration], SetAttributeOptions.defaults()
                .setTtl(ttls[iteration]).setTtlAction(TtlAction.DELETE));
          } catch (Exception e) {
            throw new RuntimeException(e);
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
    Assert.assertTrue("Execution duration " + durationMs + " took longer than expected " + LIMIT_MS,
        durationMs < LIMIT_MS);
    return errors;
  }

  private void assertErrorsSizeEquals(ConcurrentHashSet<Throwable> errors, int expected) {
    if (errors.size() != expected) {
      Assert.fail(String.format("Expected %d errors, but got %d, errors:\n",
          expected, errors.size()) + Joiner.on("\n").join(errors));
    }
  }
}
