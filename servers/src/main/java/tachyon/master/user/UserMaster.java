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

package tachyon.master.user;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TProcessor;

import tachyon.Constants;
import tachyon.master.MasterBase;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.user.journal.UserIdGeneratorEntry;
import tachyon.thrift.UserMasterService;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.io.PathUtils;

public class UserMaster extends MasterBase {

  private final AtomicLong mNextUserId = new AtomicLong(1);

  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.USER_MASTER_SERVICE_NAME);
  }

  public UserMaster(Journal journal) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("user-master-%d", true)));
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
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry instanceof UserIdGeneratorEntry) {
      mNextUserId.set(((UserIdGeneratorEntry) entry).getNextUserId());
    } else {
      throw new IOException("unexpected entry in checkpoint: " + entry);
    }
  }

  @Override
  public void writeToJournal(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(new UserIdGeneratorEntry(mNextUserId.get()));
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  @Override
  public void stop() throws IOException {
    super.stop();
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
