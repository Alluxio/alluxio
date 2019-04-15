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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This test validates concurrent create rename operations can be journaled correctly
 * and masters can replay those journals successfully when restarting.
 */
public class ConcurrentCreateRenameIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;

  private FileSystem mFileSystem;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false)
          .build();

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Test
  public void concurrentCreateRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;

    AlluxioURI path = new AlluxioURI("/foo/bar/file.inprogress");
    AlluxioURI finalPath = new AlluxioURI("/foo/bar/file");

    mFileSystem.createDirectory(new AlluxioURI("/foo/bar"),
        CreateDirectoryOptions.defaults().setRecursive(true));
    AtomicInteger successes = new AtomicInteger(0);

    FileSystemMaster fsMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(FileSystemMaster.class);

    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    List<Thread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread(() -> {
        try {
          barrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        for (int j = 0; j < 10000; j++) {
          try {
            switch (ThreadLocalRandom.current().nextInt(4)) {
              case 0:
                fsMaster.createFile(path, CreateFileOptions.defaults());
                successes.incrementAndGet();
                break;
              case 1:
                fsMaster.completeFile(path, CompleteFileOptions.defaults());
                break;
              case 2:
                fsMaster.rename(path, finalPath, RenameOptions.defaults());
                break;
              default:
                fsMaster.delete(finalPath, DeleteOptions.defaults());
                break;
            }
          } catch (Exception e) {
            // ignore and continue.
          }
        }
      });
      threads.add(t);
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    mLocalAlluxioClusterResource.get().restartMasters();
    assertEquals(1, mFileSystem.listStatus(new AlluxioURI("/")).size());
    assertTrue(successes.get() > 2);
  }
}
