package alluxio.master.file;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MetadataSyncLockManagerTest {
  private MetadataSyncLockManager mMetadataSyncLockManager;

  @Before
  public void setup() {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.MASTER_LOCK_POOL_INITSIZE, 0);
    Configuration.set(PropertyKey.MASTER_LOCK_POOL_LOW_WATERMARK, 0);
    Configuration.set(PropertyKey.MASTER_LOCK_POOL_HIGH_WATERMARK, 0);
    mMetadataSyncLockManager = new MetadataSyncLockManager();
  }

  @Test
  public void lookPoolGC() throws IOException, InterruptedException, TimeoutException {
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
  public void concurrentLock() throws IOException {
    metadataSyncLockTest("/a", "/b", false);
    metadataSyncLockTest("/a", "/a", true);
    metadataSyncLockTest("/a/b", "/a/c", false);
    metadataSyncLockTest("/a/b", "/a/b/c", true);
  }

  private void metadataSyncLockTest(String lockPath, String tryToLockPath, boolean expectBlocking)
      throws IOException {
    Closeable locks = mMetadataSyncLockManager.lockPath(new AlluxioURI(lockPath));
    CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
      try (Closeable ignored = mMetadataSyncLockManager.lockPath(new AlluxioURI(tryToLockPath))) {
        return true;
      } catch (Exception e) {
        throw new RuntimeException();
      }
    });
    try {
      future.get(100, TimeUnit.MILLISECONDS);
      assertFalse(expectBlocking);
    } catch (Exception e) {
      assertTrue(expectBlocking);
    }
    locks.close();
  }
}
