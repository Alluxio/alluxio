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
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

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
public class ConcurrentFileSystemMasterSetTtlIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /** Timeout for the concurrent test after which we will mark the test as failed. */
  private static final long LIMIT_MS = SLEEP_MS * CONCURRENCY_FACTOR / 2;
  /** The interval of the ttl bucket. */
  private static final int TTL_INTERVAL_MS = 100;

  private FileSystem mFileSystem;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, TTL_INTERVAL_MS)
          .setProperty(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          .setProperty(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          /**
           * This is to make sure master executor has enough thread to being with. Otherwise, delay
           * on master ForkJoinPool's internal thread count adjustment might take several seconds.
           * This can interfere with this test's timing expectations.
           */
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, CONCURRENCY_FACTOR)
          .build();

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
  }

  @Test
  public void rootFileConcurrentSetTtlTest() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] files = new AlluxioURI[numThreads];
    long[] ttls = new long[numThreads];
    Random random = new Random();

    for (int i = 0; i < numThreads; i++) {
      files[i] = new AlluxioURI("/file" + i);
      mFileSystem
          .createFile(files[i],
              CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build())
          .close();
      ttls[i] = random.nextInt(2 * TTL_INTERVAL_MS);
    }

    assertErrorsSizeEquals(concurrentSetTtl(files, ttls), 0);

    // Wait for all the created files being deleted after the TTLs become expired.
    CommonUtils.sleepMs(4 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);

    Assert.assertEquals("There are remaining file existing with expired TTLs",
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
            mFileSystem.setAttribute(paths[iteration], SetAttributePOptions.newBuilder()
                .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                    .setTtl(ttls[iteration]).setTtlAction(TtlAction.DELETE))
                .build());
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
