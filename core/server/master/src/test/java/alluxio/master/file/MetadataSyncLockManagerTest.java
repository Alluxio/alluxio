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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.util.CommonUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MetadataSyncLockManagerTest {
  private MetadataSyncLockManager mMetadataSyncLockManager;

  @Before
  public void setup() {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_INITSIZE, 0);
    Configuration.set(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_LOW_WATERMARK, 0);
    Configuration.set(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_HIGH_WATERMARK, 0);
    mMetadataSyncLockManager = new MetadataSyncLockManager();
  }

  @Test
  public void lookPoolGC()
      throws IOException, InterruptedException, TimeoutException, InvalidPathException {
    MetadataSyncLockManager.MetadataSyncPathList locks1 =
        mMetadataSyncLockManager.lockPath(new AlluxioURI("/a/b/c/d"));
    assertEquals(5, mMetadataSyncLockManager.getLockPoolSize());
    MetadataSyncLockManager.MetadataSyncPathList locks2 =
        mMetadataSyncLockManager.lockPath(new AlluxioURI("/e"));
    assertEquals(6, mMetadataSyncLockManager.getLockPoolSize());
    locks1.close();
    CommonUtils.waitFor("Unused locks should be recycled.",
        () -> mMetadataSyncLockManager.getLockPoolSize() == 2);
    locks2.close();
    CommonUtils.waitFor("Unused locks should be recycled.",
        () -> mMetadataSyncLockManager.getLockPoolSize() == 0);
  }

  @Test
  public void invalidPath() {
    assertThrows(InvalidPathException.class, () -> {
      try (MetadataSyncLockManager.MetadataSyncPathList ignored
               = mMetadataSyncLockManager.lockPath(new AlluxioURI("invalid path"))) {
        Void dummy;
      }
    });
    assertThrows(InvalidPathException.class, () -> {
      try (MetadataSyncLockManager.MetadataSyncPathList ignored
               = mMetadataSyncLockManager.lockPath(new AlluxioURI(" /aa/b/c "))) {
        Void dummy;
      }
    });
  }

  @Test
  public void concurrentLock()
      throws IOException, InvalidPathException, ExecutionException, InterruptedException {
    Configuration.reloadProperties();
    mMetadataSyncLockManager = new MetadataSyncLockManager();
    metadataSyncLockTest("/a", "/b", false);
    metadataSyncLockTest("/a", "/a", true);
    metadataSyncLockTest("/a/b", "/a/c", false);
    metadataSyncLockTest("/a/b", "/a/b/c", true);
    metadataSyncLockTest("/a//b//", "/a/b", true);
    metadataSyncLockTest("alluxio:///a/b", "/a/b", true);
    metadataSyncLockTest("alluxio:///a/b/", "/a/b/c", true);
  }

  private void metadataSyncLockTest(String lockPath, String tryToLockPath, boolean expectBlocking)
      throws IOException, InvalidPathException, ExecutionException, InterruptedException {
    Closeable locks = mMetadataSyncLockManager.lockPath(new AlluxioURI(lockPath));
    CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
      try (Closeable ignored = mMetadataSyncLockManager.lockPath(new AlluxioURI(tryToLockPath))) {
        return true;
      } catch (Exception e) {
        throw new RuntimeException();
      }
    });
    try {
      future.get(200, TimeUnit.MILLISECONDS);
      assertFalse(expectBlocking);
    } catch (Exception e) {
      assertTrue(expectBlocking);
    } finally {
      locks.close();
      future.get();
    }
  }
}
