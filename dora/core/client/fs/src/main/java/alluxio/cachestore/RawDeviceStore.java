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
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.file.NettyBufTargetBuffer;
import alluxio.file.ReadTargetBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

public class RawDeviceStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(RawDeviceStore.class);

  static {
    LibRawDeviceStore.loadLibrary(NativeLibraryLoader.LibCacheStoreVersion.VERSION_1);
  }

  private static final RawDeviceStore INSTANCE = new RawDeviceStore();

  private final LibRawDeviceStore LIB_RAW_DEVICE_STORE;
  private boolean mMounted;

  private RawDeviceStore() {
    LIB_RAW_DEVICE_STORE = new LibRawDeviceStore();
    mMounted = false;
  }

  public static RawDeviceStore getInstance() {
    if (!INSTANCE.mMounted) {
      synchronized (INSTANCE) {
        INSTANCE.mount();
      }
    }

    return INSTANCE;
  }

  protected void mount() {
    if (mMounted) {
      return;
    }
    final String configPath = System.getenv("CACHE_STORE_CONFIG_PATH");
    mMounted = LIB_RAW_DEVICE_STORE.openCacheStore(configPath);
    LOG.info("Mount status {}", mMounted);
  }

  public void put(PageId pageId, ByteBuffer page, boolean isTemporary)
      throws ResourceExhaustedException, IOException {
    LibRawDeviceStore.ReturnStatus returnStatus = LibRawDeviceStore.ReturnStatus.OK;
    if (page.isDirect()) {
      returnStatus= LibRawDeviceStore.ReturnStatus.fromInt(LIB_RAW_DEVICE_STORE.putPage(
          pageId.getFileId(), pageId.getPageIndex(), page, page.limit(), false));
    } else {
      returnStatus = LibRawDeviceStore.ReturnStatus.fromInt(LIB_RAW_DEVICE_STORE.putPageByteArray(
          pageId.getFileId(), pageId.getPageIndex(), page.array(), page.limit(),
          false));
    }
    LOG.debug("The buffer is direct:{}, put result {}", page.isDirect(), returnStatus);
    if (returnStatus != LibRawDeviceStore.ReturnStatus.OK) {
      throw new IOException(String.format("Failed to put %s, %d",
          pageId.getFileId(), pageId.getPageIndex()));
    }
  }

  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer buffer,
                 boolean isTemporary) throws IOException, PageNotFoundException {
    int ret = 0;
    if (buffer instanceof NettyBufTargetBuffer) {
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(
          (int)((NettyBufTargetBuffer) buffer).remaining());
      ret = LIB_RAW_DEVICE_STORE.getPage(pageId.getFileId(), pageId.getPageIndex(),
        pageOffset, bytesToRead, byteBuffer,
          false);
      if (ret >= 0) {
        byteBuffer.flip();
        byteBuffer.limit(ret);
        buffer.writeBytes(byteBuffer.array(), 0, ret);
      }
      ((DirectBuffer) byteBuffer).cleaner().clean();
    } else {
      ret = LIB_RAW_DEVICE_STORE.getPage(pageId.getFileId(), pageId.getPageIndex(),
          pageOffset, bytesToRead, buffer.byteBuffer(), false);
      if (ret >= 0) {
        buffer.byteBuffer().flip();
        buffer.byteBuffer().limit(ret);
      }
    }
    LOG.debug("Read {} from raw device store, pageOffset {}, bytesToRead {}, return size is {}",
        pageId, pageOffset, bytesToRead, ret);
    return ret;
  }

  public LibRawDeviceStore.JniPageInfo [] listPages(PageId startPageId, int batchSize) {
    return LIB_RAW_DEVICE_STORE.listPages(startPageId.getFileId(), startPageId.getPageIndex(),
        batchSize);
  }

  public void delete(PageId pageId, boolean isTemporary)
      throws IOException, PageNotFoundException {
    LOG.debug("Deletes {}", pageId);
    LIB_RAW_DEVICE_STORE.deletePage(pageId.getFileId(), pageId.getPageIndex());
  }

  public void close() throws Exception {
    LOG.debug("close the CacheStore");
    LIB_RAW_DEVICE_STORE.closeCacheStore();
  }
}