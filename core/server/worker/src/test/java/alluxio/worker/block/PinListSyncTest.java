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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class PinListSyncTest {

  private PinListSync mSync;
  private TestBlockWorker mBlockWorker;
  private FileSystemMasterClient mFileSystemMasterClient;
  private final Closer mCloser = Closer.create();

  @Before
  public void before() {
    mBlockWorker = new TestBlockWorker();
    mFileSystemMasterClient = mock(FileSystemMasterClient.class);

    mSync = mCloser.register(new PinListSync(mBlockWorker, mFileSystemMasterClient));
  }

  @Test
  public void syncPinList() throws Exception {
    Set<Long> testPinLists = ImmutableSet.of(1L, 2L, 3L);
    doReturn(testPinLists)
        .when(mFileSystemMasterClient)
        .getPinList();

    mSync.heartbeat();

    // should receive the latest pin list
    assertEquals(testPinLists, mBlockWorker.getPinList());
  }

  @Test
  public void syncPinListFailure() throws Exception {
    // simulate failure retrieving pin list from master
    doThrow(new IOException())
        .when(mFileSystemMasterClient)
        .getPinList();

    // should fail
    mSync.heartbeat();

    // should not get any pin list update
    assertEquals(ImmutableSet.of(), mBlockWorker.getPinList());
  }

  @After
  public void after() throws Exception {
    mCloser.close();
  }

  private static final class TestBlockWorker extends NoopBlockWorker {
    private Set<Long> mLatestPinList = ImmutableSet.of();

    @Override
    public void updatePinList(Set<Long> pinList) {
      mLatestPinList = ImmutableSet.copyOf(pinList);
    }

    public Set<Long> getPinList() {
      return mLatestPinList;
    }
  }
}
