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

package alluxio.master.file.meta;

import static org.junit.Assert.assertFalse;

import alluxio.concurrent.LockMode;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link InodeLockManager}.
 */
public class InodeLockManagerTest {
  @Test(timeout = 10000)
  public void lockInode() throws Exception {
    inodeLockTest(LockMode.WRITE, LockMode.READ, true);
    inodeLockTest(LockMode.READ, LockMode.WRITE, true);
    inodeLockTest(LockMode.WRITE, LockMode.WRITE, true);
    inodeLockTest(LockMode.READ, LockMode.READ, false);
  }

  @Test(timeout = 10000)
  public void lockEdge() throws Exception {
    edgeLockTest(LockMode.WRITE, LockMode.READ, true);
    edgeLockTest(LockMode.READ, LockMode.WRITE, true);
    edgeLockTest(LockMode.WRITE, LockMode.WRITE, true);
    edgeLockTest(LockMode.READ, LockMode.READ, false);
  }

  private void inodeLockTest(LockMode take, LockMode tryToTake, boolean expectBlocking)
      throws Exception {
    InodeLockManager lockManager = new InodeLockManager();
    AtomicBoolean threadFinished = new AtomicBoolean(false);
    MutableInodeFile inode = MutableInodeFile.create(0, 0, "name", 0, CreateFileContext.defaults());
    LockResource lock = lockManager.lockInode(inode, take, false);
    Thread t = new Thread(() -> {
      // Copy the inode to make sure we aren't comparing inodes by reference.
      MutableInodeFile inodeCopy =
          MutableInodeFile.fromJournalEntry(inode.toJournalEntry().getInodeFile());
      try (LockResource lr = lockManager.lockInode(inodeCopy, tryToTake, false)) {
        threadFinished.set(true);
      }
    });
    t.start();
    if (expectBlocking) {
      CommonUtils.sleepMs(20);
      assertFalse(threadFinished.get());
      lock.close();
    }
    CommonUtils.waitFor("lock to be acquired by the second thread", threadFinished::get);
  }

  private void edgeLockTest(LockMode take, LockMode tryToTake, boolean expectBlocking)
      throws Exception {
    InodeLockManager lockManager = new InodeLockManager();
    AtomicBoolean threadFinished = new AtomicBoolean(false);
    LockResource lock = lockManager.lockEdge(new Edge(10, "name"), take, false);
    Thread t = new Thread(() -> {
      // Use a new Edge each time to make sure we aren't comparing edges by reference.
      try (LockResource lr = lockManager.lockEdge(new Edge(10, "name"), tryToTake, false)) {
        threadFinished.set(true);
      }
    });
    t.start();
    if (expectBlocking) {
      CommonUtils.sleepMs(20);
      assertFalse(threadFinished.get());
      lock.close();
    }
    CommonUtils.waitFor("lock to be acquired by the second thread", () -> threadFinished.get());
  }
}
