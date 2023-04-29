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

package alluxio.master.mdsync;

import alluxio.AlluxioURI;
import alluxio.conf.path.TrieNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DirectoryPathWaiter extends BaseTask implements PathWaiter {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryPathWaiter.class);

  private final TrieNode<AlluxioURI> mCompletedDirs = new TrieNode<>();

  DirectoryPathWaiter(
      TaskInfo info, long startTime) {
    super(info, startTime);
  }

  @Override
  public synchronized boolean waitForSync(AlluxioURI path) {
    while (true) {
      if (mIsCompleted != null) {
        return !mIsCompleted.getResult().isPresent();
      }
      boolean completed = mCompletedDirs.getClosestTerminal(path.getPath())
          .map(result -> {
            if (result.getValue().equals(path)) {
              return true;
            }
            AlluxioURI parent = path.getParent();
            return parent != null && parent.equals(result.getValue());
          }).orElse(false);
      if (completed) {
        return true;
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
    if (!completed.isTruncated()) {
      mCompletedDirs.insert(completed.getBaseLoadPath().getPath())
          .setValue(completed.getBaseLoadPath());
      notifyAll();
    }
  }
}
