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

package tachyon.master.file.journal;

import java.util.Map;

import com.google.common.collect.Maps;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for recording the entry of setting TTL.
 */
public class SetTTLEntry implements JournalEntry {
  private final long mId;
  private final long mTTL;

  /**
   * Creates a new instance of <code>SetTTLEntry</code>.
   *
   * @param id the id of the entry
   * @param ttl the ttl value to be set
   */
  public SetTTLEntry(long id, long ttl) {
    mId = id;
    mTTL = ttl;
  }

  public long getId() {
    return mId;
  }

  public long getTTL() {
    return mTTL;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.SET_TTL;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(2);
    parameters.put("id", mId);
    parameters.put("ttl", mTTL);
    return parameters;
  }
}
