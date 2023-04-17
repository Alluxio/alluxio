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

package alluxio.master.metastore.rocks;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.master.journal.checkpoint.CheckpointInputStream;

import alluxio.util.SleepUtils;
import alluxio.util.ThreadFactoryUtils;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class RocksStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private RocksStore mStore;
  List<RocksObject> mToClose;
  AtomicReference<ColumnFamilyHandle> mTestColumn;
  String mDbDir;
  String mBackupsDir;
  List<ColumnFamilyDescriptor> mColumnDescriptors;
  ExecutorService mThreadPool;

  @Before
  public void setup() throws Exception {
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "500ms");
    Configuration.set(PropertyKey.TEST_MODE, true);

    mToClose = new ArrayList<>();
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
            .setMemTableConfig(new HashLinkedListMemTableConfig())
            .setCompressionType(CompressionType.NO_COMPRESSION)
            .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key
    mToClose.add(cfOpts);

    mColumnDescriptors =
            Arrays.asList(new ColumnFamilyDescriptor("test".getBytes(), cfOpts));
    mDbDir = mFolder.newFolder("rocks").getAbsolutePath();
    mBackupsDir = mFolder.newFolder("rocks-backups").getAbsolutePath();
    mTestColumn = new AtomicReference<>();
    DBOptions dbOpts = new DBOptions().setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setAllowConcurrentMemtableWrite(false);
    mToClose.add(dbOpts);

    mStore = new RocksStore("test", mDbDir, mBackupsDir, dbOpts, mColumnDescriptors,
                    Arrays.asList(mTestColumn));

    mThreadPool = Executors.newCachedThreadPool(ThreadFactoryUtils.build("test-executor-%d", true));
  }

  @After
  public void tearDown() throws Exception {
    try (RocksExclusiveLockHandle lock = mStore.lockForClosing()) {
      mStore.close();
    }

    Collections.reverse(mToClose);
    mToClose.forEach(RocksObject::close);

    mThreadPool.shutdownNow();
  }

  @Test
  public void backupRestore() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RocksDB db;
    int count = 10;
    try (RocksSharedLockHandle lock = mStore.checkAndAcquireSharedLock()) {
      db = mStore.getDb();
      for (int i = 0; i < count; i++) {
        db.put(mTestColumn.get(), new WriteOptions().setDisableWAL(true), ("a" + i).getBytes(),
            "b".getBytes());
      }
    }
    try (RocksExclusiveLockHandle lock = mStore.lockForCheckpoint()) {
      mStore.writeToCheckpoint(baos);
    }
    try (RocksExclusiveLockHandle lock = mStore.lockForClosing()) {
      mStore.close();
    }

    String newDbDir = mFolder.newFolder("rocks-new").getAbsolutePath();
    DBOptions dbOpts = new DBOptions().setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setAllowConcurrentMemtableWrite(false);
    mToClose.add(dbOpts);
    mStore =
        new RocksStore("test-new", newDbDir, mBackupsDir, dbOpts, mColumnDescriptors,
            Arrays.asList(mTestColumn));
    try (RocksExclusiveLockHandle lock = mStore.lockForRewrite()) {
      mStore.restoreFromCheckpoint(
          new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())));
    }
    try (RocksSharedLockHandle lock = mStore.checkAndAcquireSharedLock()) {
      db = mStore.getDb();
      for (int i = 0; i < count; i++) {
        assertArrayEquals("b".getBytes(), db.get(mTestColumn.get(), ("a" + i).getBytes()));
      }
    }
  }

  @Test
  public void sharedLockRefCount() {
    List<RocksSharedLockHandle> readLocks = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      assertEquals(i, mStore.getSharedLockCount());
      RocksSharedLockHandle lockHandle = mStore.checkAndAcquireSharedLock();
      readLocks.add(lockHandle);
    }
    assertEquals(20, mStore.getSharedLockCount());

    for (int i = 0; i < 20; i++) {
      assertEquals(20 - i, mStore.getSharedLockCount());
      readLocks.get(i).close();
    }
    assertEquals(0, mStore.getSharedLockCount());
  }

  @Test
  public void exclusiveLockOnClosing() {
    RocksExclusiveLockHandle exclusiveLock = mStore.lockForClosing();

    Exception e = assertThrows(UnavailableRuntimeException.class, () -> {
      mStore.checkAndAcquireSharedLock();
    });
    assertTrue(e.getMessage().contains(ExceptionMessage.ROCKS_DB_CLOSING.getMessage()));
    Exception f = assertThrows(UnavailableRuntimeException.class, () -> {
      mStore.shouldAbort(0);
    });
    assertTrue(f.getMessage().contains(ExceptionMessage.ROCKS_DB_CLOSING.getMessage()));
    assertEquals(0, mStore.getSharedLockCount());
    assertTrue(mStore.isServiceStopping());
    exclusiveLock.close();
    assertEquals(0, mStore.getSharedLockCount());
    // The flag is NOT reset after the lock is released, because the service will exit
    assertTrue(mStore.isServiceStopping());
  }

  @Test
  public void exclusiveLockOnCheckpoint() {
    RocksExclusiveLockHandle exclusiveLock = mStore.lockForCheckpoint();

    Exception e = assertThrows(UnavailableRuntimeException.class, () -> {
      mStore.checkAndAcquireSharedLock();
    });
    assertTrue(e.getMessage().contains(ExceptionMessage.ROCKS_DB_CLOSING.getMessage()));
    Exception f = assertThrows(UnavailableRuntimeException.class, () -> {
      mStore.shouldAbort(0);
    });
    assertTrue(f.getMessage().contains(ExceptionMessage.ROCKS_DB_CLOSING.getMessage()));
    assertEquals(0, mStore.getSharedLockCount());
    assertTrue(mStore.isServiceStopping());
    exclusiveLock.close();
    assertEquals(0, mStore.getSharedLockCount());
    // The flag is NOT reset after the lock is released, because the service will exit
    assertFalse(mStore.isServiceStopping());
  }

  @Test
  public void exclusiveLockForced() throws Exception {
    int refCountTrackerVersion = mStore.getRefCountVersion();
    // One reader gets the shared lock and does not release for a long time
    CountDownLatch mReaderCloseLatch = new CountDownLatch(1);
    CountDownLatch mWriterStartLatch = new CountDownLatch(1);
    Future<Void> f = mThreadPool.submit(() -> {
      RocksSharedLockHandle lockHandle = mStore.checkAndAcquireSharedLock();
      System.out.println("Read lock grabbed");
      mWriterStartLatch.countDown();
      assertEquals(1, mStore.getSharedLockCount());
      try {
        mReaderCloseLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      System.out.println("Able to unlock read lock now");
      // After a long time, this lock is released after the exclusive lock has been forced
      lockHandle.close();
      System.out.println("Read lock released");
      // The lock release should not mess up the ref count
      assertEquals(0, mStore.getSharedLockCount());
      return null;
    });

    // One closer comes in and eventually will grab the lock after wait
    // TODO(jiacheng): UT for lockForClosing
    mWriterStartLatch.await();
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    RocksExclusiveLockHandle exclusiveLock = mStore.lockForCheckpoint();
    // After some wait, the closer will force the lock and reset the ref count
    // And the ref count will be reset on that force
    assertEquals(0, mStore.getSharedLockCount());
    assertTrue(mStore.getRefCountVersion() > refCountTrackerVersion);
    // Let the reader finish
    mReaderCloseLatch.countDown();
    f.get();
    exclusiveLock.close();
    assertEquals(0, mStore.getSharedLockCount());
  }

  @Test
  public void forcingExclusiveLockInTestWillErr() throws Exception {
    int refCountTrackerVersion = mStore.getRefCountVersion();
    // One reader gets the shared lock and does not release for a long time
    CountDownLatch mReaderCloseLatch = new CountDownLatch(1);
    CountDownLatch mWriterStartLatch = new CountDownLatch(1);
    Future<Void> f = mThreadPool.submit(() -> {
      RocksSharedLockHandle lockHandle = mStore.checkAndAcquireSharedLock();
      System.out.println("Read lock grabbed");
      mWriterStartLatch.countDown();
      assertEquals(1, mStore.getSharedLockCount());
      try {
        mReaderCloseLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      System.out.println("Able to unlock read lock now");
      // After a long time, this lock is released after the exclusive lock has been forced
      lockHandle.close();
      System.out.println("Read lock released");
      // The lock release should not mess up the ref count
      assertEquals(0, mStore.getSharedLockCount());
      return null;
    });

    // One closer comes in and eventually will grab the lock after wait
    // TODO(jiacheng): UT for lockForClosing
    mWriterStartLatch.await();
    // In test mode, forcing the exclusive lock will result in an exception
    // This will help us detect issues with the ref count
    assertThrows(RuntimeException.class, () -> {
      RocksExclusiveLockHandle exclusiveLock = mStore.lockForCheckpoint();
    });
    // Let the reader finish
    mReaderCloseLatch.countDown();
    f.get();
    // Even if the exclusive lock attempt failed, the ref count will be correct
    assertEquals(0, mStore.getSharedLockCount());
    assertEquals(refCountTrackerVersion, mStore.getRefCountVersion());
  }

  @Test
  public void readerCanContinueAfterCheckpoint() throws Exception {
    int refCountTrackerVersion = mStore.getRefCountVersion();
    // One reader gets the shared lock and does not release for a long time
    CountDownLatch mReaderCloseLatch = new CountDownLatch(1);
    CountDownLatch mWriterStartLatch = new CountDownLatch(1);
    Future<Void> f = mThreadPool.submit(() -> {
      RocksSharedLockHandle lockHandle = mStore.checkAndAcquireSharedLock();
      System.out.println("Read lock grabbed");
      mWriterStartLatch.countDown();
      try {
        mReaderCloseLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      // While this reader is sleeping, one checkpoint is completed in the background
      // This check should pass without throwing an exception
      // And that means the reader can continue doing what it was doing
      mStore.shouldAbort(lockHandle.mDbVersion);

      System.out.println("Able to continue reading");
      // After finishing its work, this lock is released
      lockHandle.close();
      System.out.println("Read lock released");
      // The lock release has passed due but should not mess up the ref count
      assertEquals(0, mStore.getSharedLockCount());
      return null;
    });

    // One closer comes in and eventually will grab the lock after wait
    mWriterStartLatch.await();
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    RocksExclusiveLockHandle exclusiveLock = mStore.lockForCheckpoint();
    // After some wait, the closer will force the lock and reset the ref count
    // And the ref count will be reset on that force
    assertEquals(0, mStore.getSharedLockCount());
    assertTrue(mStore.getRefCountVersion() > refCountTrackerVersion);
    // Now the checkpointing was done, while the reader is still asleep
    exclusiveLock.close();
    // Let the reader wake up and continue
    mReaderCloseLatch.countDown();
    f.get();
    assertEquals(0, mStore.getSharedLockCount());
    assertTrue(mStore.getRefCountVersion() > refCountTrackerVersion);
  }
}
