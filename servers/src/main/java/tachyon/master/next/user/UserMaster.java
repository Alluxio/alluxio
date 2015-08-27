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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.next.MasterBase;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalOutputStream;
import tachyon.thrift.UserMasterService;

public class UserMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final AtomicInteger mNextUserId = new AtomicInteger(1);

  public UserMaster(Journal journal) {
    super(journal);
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
  public void processJournalCheckpoint(JournalInputStream inputStream) {
    // TODO
  }

  @Override
  public void processJournalEntry(JournalEntry entry) {
    // TODO
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
  public void writeJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // TODO(cc)
  }

  public long getUserId() {
    synchronized (mNextUserId) {
      // TODO: journal
      return mNextUserId.getAndIncrement();
    }
  }
}
