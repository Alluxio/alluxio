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
import alluxio.master.Master;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
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
  public Map<String, TProcessor> getServices() {
    return null;
  }

  @Override
  public String getName() {
    return "FakeMaster";
  }

  @Override
  @Nullable
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
    mEntries.add(entry);
  }

  @Override
  public void resetState() {}

  @Override
  public void start(Boolean isPrimary) throws IOException {}

  @Override
  public void stop() throws IOException {}

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return mEntries.iterator();
  }
}

