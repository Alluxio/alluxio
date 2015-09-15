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

package tachyon.master.lineage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.thrift.TProcessor;

import tachyon.Constants;
import tachyon.client.file.TachyonFile;
import tachyon.job.Job;
import tachyon.master.MasterBase;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.thrift.LineageMasterService;
import tachyon.util.ThreadFactoryUtils;

/**
 * The lineage master stores the lineage metadata in Tachyon, and it contains the components that
 * manage all lineage-related activities.
 */
public final class LineageMaster extends MasterBase {
  private final LineageStore mLineageStore;

  public LineageMaster(Journal journal) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("file-system-master-%d", true)));

    mLineageStore = new LineageStore();
  }

  @Override
  public TProcessor getProcessor() {
    return new LineageMasterService.Processor<LineageMasterServiceHandler>(
        new LineageMasterServiceHandler(this));
  }

  @Override
  public String getServiceName() {
    return Constants.LINEAGE_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO add journal support
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // TODO add journal support
  }

  public long createLineage(List<TachyonFile> inputFiles, List<TachyonFile> outputFiles, Job job) {
    // TODO create lineage
    return -1;
  }

  public boolean deleteLineage(long lineageId) {
    // TODO delete lineage
    return false;
  }
}
