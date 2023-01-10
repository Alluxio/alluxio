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

package alluxio.client.file.cache;

import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.exception.PageNotFoundException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A PageStore can hang on put, get or delete.
 */
class HangingPageStore extends LocalPageStore {
  private AtomicBoolean mDeleteHanging = new AtomicBoolean(false);
  private AtomicBoolean mGetHanging = new AtomicBoolean(false);
  private AtomicBoolean mPutHanging = new AtomicBoolean(false);
  private AtomicInteger mPut = new AtomicInteger(0);
  private AtomicLong mStopHangingThread = new AtomicLong(-1);

  public HangingPageStore(PageStoreOptions options) {
    super(options);
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    // never quit
    while (mDeleteHanging.get()) {}
    super.delete(pageId);
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, PageReadTargetBuffer target,
      boolean isTemporary)
      throws IOException, PageNotFoundException {
    checkStopHanging();
    // never quit
    while (mGetHanging.get()) {}
    return super.get(pageId, pageOffset, bytesToRead, target, isTemporary);
  }

  @Override
  public void put(PageId pageId, ByteBuffer page, boolean isTemporary) throws IOException {
    checkStopHanging();
    // never quit
    while (mPutHanging.get()) {}
    super.put(pageId, page, isTemporary);
    mPut.getAndIncrement();
  }

  private void checkStopHanging() {
    if (mStopHangingThread.get() == Thread.currentThread().getId()) {
      mPutHanging.set(false);
      mGetHanging.set(false);
    }
  }

  /**
   * @param value if delete operation hangs
   */
  public void setDeleteHanging(boolean value) {
    mDeleteHanging.set(value);
  }

  /**
   * @param value if get operation hangs
   */
  public void setGetHanging(boolean value) {
    mGetHanging.set(value);
  }

  /**
   * @param value if put operation hangs
   */
  public void setPutHanging(boolean value) {
    mPutHanging.set(value);
  }

  /**
   * Set a thread id so that if a thread with the given id reaches
   * the line where it should hang, it will disable hanging.
   * @param id the thread id to stop the hanging
   */
  public void setStopHangingThread(long id) {
    mStopHangingThread.set(id);
  }

  /**
   * @return number of put operations
   */
  public int getPuts() {
    return mPut.get();
  }
}
