/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.user;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.next.MasterBase;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalOutputStream;
import tachyon.master.next.user.journal.UserIdGeneratorEntry;
import tachyon.thrift.UserMasterService;

public class UserMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final AtomicLong mNextUserId = new AtomicLong(1);

  public UserMaster(Journal journal, ExecutorService executorService) {
    super(journal, executorService);
  }

  @Override
  public TProcessor getProcessor() {
    return new UserMasterService.Processor<UserMasterServiceHandler>(
        new UserMasterServiceHandler(this));
  }

  @Override
  public String getProcessorName() {
    return Constants.USER_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    JournalEntry entry;
    while ((entry = inputStream.getNextEntry()) != null) {
      processJournalEntry(entry);
    }
    inputStream.close();
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry instanceof UserIdGeneratorEntry) {
      mNextUserId.set(((UserIdGeneratorEntry) entry).getNextUserId());
    } else {
      throw new IOException("unexpected entry in checkpoint: " + entry);
    }
  }

  @Override
  public void start(boolean asMaster) throws IOException {
    startMaster(asMaster);
  }

  @Override
  public void stop() throws IOException {
    stopMaster();
  }

  @Override
  public void writeToJournal(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(new UserIdGeneratorEntry(mNextUserId.get()));
  }

  public long getUserId() {
    synchronized (mNextUserId) {
      long userId = mNextUserId.getAndIncrement();
      writeJournalEntry(new UserIdGeneratorEntry(userId));
      flushJournal();
      return userId;
    }
  }
}
