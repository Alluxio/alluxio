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

package alluxio.master.journal.raft;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link RaftJournalWriter}.
 */
public class RaftJournalWriterTest {
  private LocalFirstRaftClient mClient;
  private RaftJournalWriter mRaftJournalWriter;

  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  private void setupRaftJournalWriter() throws IOException  {
    mClient = mock(LocalFirstRaftClient.class);
    RaftClientReply reply = RaftClientReply.newBuilder()
            .setClientId(ClientId.randomId())
            .setServerId(
              RaftGroupMemberId.valueOf(RaftJournalUtils.getPeerId(new InetSocketAddress(1)),
              RaftGroupId.valueOf(UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1")))
            ).setCallId(1L)
            .setSuccess(true)
            .setMessage(Message.valueOf("mp"))
            .setException(null)
            .setLogIndex(1L)
            .setCommitInfos(null)
            .build();

    CompletableFuture<RaftClientReply> future = new CompletableFuture<RaftClientReply>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public RaftClientReply get() {
        return reply;
      }

      @Override
      public RaftClientReply get(long timeout, TimeUnit unit) {
        return reply;
      }
    };
    when(mClient.sendAsync(any(), any())).thenReturn(future);

    mRaftJournalWriter = new RaftJournalWriter(1, mClient);
  }

  @Test
  public void writeAndFlush() throws Exception {
    setupRaftJournalWriter();
    for (int i = 0; i < 10; i++) {
      String alluxioMountPoint = "/tmp/to/file" + i;
      String ufsPath = "hdfs://location/file" + i;
      mRaftJournalWriter.write(Journal.JournalEntry.newBuilder()
          .setAddMountPoint(File.AddMountPointEntry.newBuilder()
              .setAlluxioPath(alluxioMountPoint)
              .setUfsPath(ufsPath).build()).build());
    }
    verify(mClient, never()).sendAsync(any(), any());

    mRaftJournalWriter.flush();
    verify(mClient, times(1)).sendAsync(any(), any());
    mRaftJournalWriter.flush();
    verify(mClient, times(1)).sendAsync(any(), any());

    mRaftJournalWriter.write(Journal.JournalEntry.getDefaultInstance());
    mRaftJournalWriter.flush();
    verify(mClient, times(2)).sendAsync(any(), any());
  }

  @Test
  public void writeTriggerFlush() throws Exception {
    int flushBatchSize = 128;
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ENTRY_SIZE_MAX, flushBatchSize * 3);
    setupRaftJournalWriter();

    int totalMessageBytes = 0;
    for (int i = 0; i < 10; i++) {
      String alluxioMountPoint = "/tmp/to/file" + i;
      String ufsPath = "hdfs://location/file" + i;
      totalMessageBytes += alluxioMountPoint.getBytes().length;
      totalMessageBytes += ufsPath.getBytes().length;
      mRaftJournalWriter.write(Journal.JournalEntry.newBuilder()
          .setAddMountPoint(File.AddMountPointEntry.newBuilder()
              .setAlluxioPath(alluxioMountPoint)
              .setUfsPath(ufsPath).build()).build());
    }
    mRaftJournalWriter.write(Journal.JournalEntry.getDefaultInstance());
    verify(mClient, atLeast(totalMessageBytes / flushBatchSize)).sendAsync(any(), any());
  }
}
