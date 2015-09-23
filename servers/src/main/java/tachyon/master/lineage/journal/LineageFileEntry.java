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

package tachyon.master.lineage.journal;

import java.util.Map;

import com.google.common.collect.Maps;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageFileState;

public class LineageFileEntry implements JournalEntry {
  private long mFileId;
  private LineageFileState mState;
  private String mUnderFilePath;

  public LineageFileEntry(long fileId, LineageFileState state, String underFilePath) {
    mFileId = fileId;
    mState = state;
    mUnderFilePath = underFilePath;
  }

  public LineageFile toLineageFile() {
    return new LineageFile(mFileId, mState, mUnderFilePath);
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.LINEAGE_FILE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(3);
    parameters.put("fileId", mFileId);
    parameters.put("state", mState);
    parameters.put("underFilePath", mUnderFilePath);
    return parameters;
  }
}
