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

public final class DeleteLineageEntry implements JournalEntry {
  private long mLineageId;
  private boolean mCascade;

  public DeleteLineageEntry(long lineageId, boolean cascade) {
    mLineageId = lineageId;
    mCascade = cascade;
  }

  public long getLineageId() {
    return mLineageId;
  }

  public boolean isCascade() {
    return mCascade;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.DELETE_LINEAGE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(2);
    parameters.put("lineageId", mLineageId);
    parameters.put("cascade", mCascade);
    return parameters;
  }
}
