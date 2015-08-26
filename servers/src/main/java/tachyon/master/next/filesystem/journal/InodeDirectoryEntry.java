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

package tachyon.master.next.filesystem.journal;

import java.util.List;
import java.util.Map;

import tachyon.master.next.journal.JournalEntryType;

public class InodeDirectoryEntry extends InodeEntry {
  private List<Long> mChildrenIds;

  public InodeDirectoryEntry(long creationTimeMs, long id, String name, long parentId,
      boolean isPinned, long lastModificationTimeMs, List<Long> childrenIds) {
    super(creationTimeMs, id, name, parentId, isPinned, lastModificationTimeMs);

    mChildrenIds = childrenIds;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.INODE_DIRECTORY;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = super.getParameters();
    parameters.put("childrenIds", mChildrenIds);
    return parameters;
  }
}
