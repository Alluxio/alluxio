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

package alluxio.master;

import alluxio.Server;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * A fake master implementation.
 */
public final class MockMaster implements Master {
  private Queue<JournalEntry> mEntries;

  public MockMaster() {
    mEntries = new ArrayDeque<>();
  }

  @Override
  @Nullable
  public Map<ServiceType, GrpcService> getServices() {
    return null;
  }

  @Override
  public String getName() {
    return "MockMaster";
  }

  @Override
  @Nullable
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public void start(Boolean isPrimary) {}

  @Override
  public void stop() {}

  @Override
  public void close() {}

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    mEntries.add(entry);
    return true;
  }

  @Override
  public void resetState() {}

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(mEntries.iterator());
  }

  @Override
  public JournalContext createJournalContext() {
    throw new IllegalStateException("Cannot create journal contexts for MockMaster");
  }

  @Override
  public MasterContext getMasterContext() {
    return null;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.NOOP;
  }
}

