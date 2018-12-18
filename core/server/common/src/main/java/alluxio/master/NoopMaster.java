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
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.Journal.JournalEntry;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Master implementation that does nothing. This is useful for testing and for situations where we
 * don't want to run a real master, e.g. when formatting the journal.
 */
public class NoopMaster implements Master {
  private final String mName;

  /**
   * Creates a new {@link NoopMaster}.
   */
  public NoopMaster() {
    this("NoopMaster");
  }

  /**
   * Creates a new {@link NoopMaster} with the given name.
   *
   * @param name the master name
   */
  public NoopMaster(String name) {
    mName = name;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return null;
  }

  @Override
  public void start(Boolean options) throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return null;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
  }

  @Override
  public void resetState() {
  }

  @Override
  public JournalContext createJournalContext() {
    throw new IllegalStateException("Cannot create journal contexts for NoopMaster");
  }
}
