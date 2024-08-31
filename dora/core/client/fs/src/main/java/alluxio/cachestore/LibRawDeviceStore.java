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

package alluxio.cachestore;

import alluxio.cachestore.utils.NativeLibraryLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class LibRawDeviceStore {

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static AtomicReference<LibraryState> libraryLoaded =
      new AtomicReference<>(LibraryState.NOT_LOADED);

  public enum ReturnStatus {
    OK,
    BENIGN_RACING,
    INSUFFICIENT_SPACE_EVICTED,
    NO_SPACE_LEFT,
    OTHER;

    public static ReturnStatus fromInt(int i) {
      switch (i) {
        case 0:
          return OK;
        case 1:
          return BENIGN_RACING;
        case 2:
          return INSUFFICIENT_SPACE_EVICTED;
        case 3:
          return NO_SPACE_LEFT;
        case 4:
          return OTHER;
        default:
          return OTHER;
      }
    }
  };

  public static class JniPageInfo {
    private final String mFileId;
    private final long mPageId;

    private final long mFileSize;

    private final long mCreationTime;

    public JniPageInfo(String fileId, long pageId, long fileSize, long creationTime) {
      mFileId = fileId;
      mPageId = pageId;
      mFileSize = fileSize;
      mCreationTime = creationTime;
    }

    public String getFileId() {
      return mFileId;
    }

    public long getPageId() {
      return mPageId;
    }

    public long getFileSize() {
      return mFileSize;
    }

    public long getCreationTime() {
      return mCreationTime;
    }
  }
  public native boolean openCacheStore(String storePath);
  public native boolean closeCacheStore();
  public native int putPage(String fileId, long pageId, ByteBuffer page, int size, boolean isTemporary);

  public native int putPageByteArray(String fileId, long pageId, byte [] page, int size, boolean isTemporary);

  public native int getPage(String fileId, long pageId, int pageOffset, int bytesToRead,
      ByteBuffer buffer, int bufStartPos, boolean isTemporary);
  public native boolean deletePage(String fileId, long pageId);

  public native boolean commitPage(String fileId, String newFileId);

  public native JniPageInfo[] listPages(String startFileId, long startPageId, int batchSize);

  public static void loadLibrary(NativeLibraryLoader.LibCacheStoreVersion version) {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      String tmpDir = System.getenv("JNICACHESTORE_SHAREDLIB_DIR");
      try {
        NativeLibraryLoader.getInstance().loadLibrary(version, tmpDir);
      } catch (IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the jni-fuse shared library"
            + e);
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }
}
