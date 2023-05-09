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

package alluxio.master.file.mdsync;

import alluxio.AlluxioURI;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

class BatchPathWaiter extends BaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(BatchPathWaiter.class);
  private static final AlluxioURI EMPTY = new AlluxioURI("");

  final List<PathSequence> mLastCompleted;
  final PathSequence mNoneCompleted;

  BatchPathWaiter(
      TaskInfo info, long startTime,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier,
      boolean removeOnComplete) {
    super(info, startTime, clientSupplier, removeOnComplete);
    mNoneCompleted = new PathSequence(EMPTY, info.getAlluxioPath());
    mLastCompleted = Lists.newArrayList(mNoneCompleted);
  }

  @VisibleForTesting
  List<PathSequence> getLastCompleted() {
    return mLastCompleted;
  }

  @Override
  public synchronized boolean waitForSync(AlluxioURI path) {
    while (true) {
      if (mIsCompleted != null) {
        return mIsCompleted.succeeded();
      }
      PathSequence minCompleted = mLastCompleted.get(0);
      if (minCompleted != mNoneCompleted) {
        if (minCompleted.getStart().compareTo(path) <= 0
            && minCompleted.getEnd().compareTo(path) > 0) {
          return true;
        }
      }
      try {
        wait();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for synced path {}", path);
        return false;
      }
    }
  }

  @Override
  public synchronized void nextCompleted(SyncProcessResult completed) {
    if (!completed.getLoaded().isPresent()) {
      return;
    }
    PathSequence loaded = completed.getLoaded().get();
    AlluxioURI newRight = null;
    AlluxioURI newLeft = null;
    int i = 0;
    for (; i < mLastCompleted.size(); i++) {
      int rightCmp = mLastCompleted.get(i).getStart().compareTo(loaded.getEnd());
      if (rightCmp == 0) {
        newRight = mLastCompleted.get(i).getEnd();
      }
      if (rightCmp >= 0) {
        break;
      }
      int leftCmp = mLastCompleted.get(i).getEnd().compareTo(loaded.getStart());
      if (leftCmp == 0) {
        newLeft = mLastCompleted.get(i).getStart();
      }
    }
    if (newRight == null && newLeft == null) {
      mLastCompleted.add(i, loaded);
    } else if (newRight != null && newLeft != null) {
      mLastCompleted.set(i, new PathSequence(newLeft, newRight));
      mLastCompleted.remove(i - 1);
    } else if (newLeft != null) {
      mLastCompleted.set(i - 1, new PathSequence(newLeft, loaded.getEnd()));
    } else {
      mLastCompleted.set(i, new PathSequence(loaded.getStart(), newRight));
    }
    notifyAll();
  }
}
