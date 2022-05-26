package alluxio.client.file.cache.store;

import alluxio.resource.LockResource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

abstract class AbstractPageStoreDir implements PageStoreDir {

  private final ReentrantReadWriteLock mBlockPageMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mFileIdSetLock")
  private final Set<String> mFileIdSet = new HashSet<>();

  private final Path mRootPath;
  private final long mCapacity;
  private final AtomicLong mBytesUsed = new AtomicLong(0);

  AbstractPageStoreDir(Path rootPath, long capacity) {
    mRootPath = rootPath;
    mCapacity = capacity;
  }

  @Override
  public Path getRootPath() {
    return mRootPath;
  }

  @Override
  public long getCapacity() {
    return mCapacity;
  }

  @Override
  public long getCachedBytes() {
    return 0;
  }

  @Override
  public boolean addFileToDir(String fileId) {
    try (LockResource lock = new LockResource(mBlockPageMapLock.writeLock())) {
      return mFileIdSet.add(fileId);
    }
  }

  @Override
  public boolean reserveSpace(int bytes) {
    long previousBytesUsed;
    do {
      previousBytesUsed = mBytesUsed.get();
      if (previousBytesUsed + bytes > mCapacity) {
        return false;
      }
    } while (!mBytesUsed.compareAndSet(previousBytesUsed, previousBytesUsed + bytes));
    return true;
  }

  @Override
  public long releaseSpace(int bytes) {
    return mBytesUsed.addAndGet(bytes);
  }

  @Override
  public boolean hasFile(String fileId) {
    try (LockResource lock = new LockResource(mBlockPageMapLock.readLock())) {
      return mFileIdSet.contains(fileId);
    }
  }
}
