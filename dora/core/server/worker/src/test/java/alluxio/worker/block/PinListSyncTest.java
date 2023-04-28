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

import alluxio.ClientContext;
import alluxio.conf.Configuration;
import alluxio.master.MasterClientContext;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class PinListSyncTest {

  // context for creating FileSystemMasterClient
  private final MasterClientContext mContext = MasterClientContext.newBuilder(ClientContext.create(
      Configuration.global())).build();

  private final TestBlockWorker mBlockWorker = new TestBlockWorker();

  @Test
  public void syncPinList() {
    Set<Long> testPinLists = ImmutableSet.of(1L, 2L, 3L);
    // simulates pin list update returned by master
    FileSystemMasterClient client = new FileSystemMasterClient(mContext) {
      @Override
      public Set<Long> getPinList() {
        return ImmutableSet.copyOf(testPinLists);
      }
    };

    PinListSync sync = new PinListSync(mBlockWorker, client);
    sync.heartbeat();

    // should receive the latest pin list
    assertEquals(testPinLists, mBlockWorker.getPinList());
  }

  @Test
  public void syncPinListFailure() {
    // simulates get pin list failure
    FileSystemMasterClient client = new FileSystemMasterClient(mContext) {
      @Override
      public Set<Long> getPinList() throws IOException {
        throw new IOException();
      }
    };

    PinListSync sync = new PinListSync(mBlockWorker, client);
    // should fail
    sync.heartbeat();

    // should not get any pin list update
    assertEquals(ImmutableSet.of(), mBlockWorker.getPinList());
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
