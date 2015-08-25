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
import java.util.Set;

import com.google.common.collect.Lists;

import tachyon.master.next.filesystem.meta.Inode;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalEntryType;

public class InodeDirectoryEntry extends InodeEntry {
  private Set<Inode> mChildren;

  public InodeDirectoryEntry(long creationTimeMs, long id, String name, long parentId,
      boolean isPinned, long lastModificationTimeMs, Set<Inode> children) {
    super(creationTimeMs, id, name, parentId, isPinned, lastModificationTimeMs);
    mChildren = children;
  }

  @Override
  public JournalEntryType type() {
    return JournalEntryType.INODE_DIRECTORY;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = super.getParameters();
    List<Long> childrenIds = Lists.newArrayListWithCapacity(mChildren.size());
    for (Inode child : mChildren) {
      childrenIds.add(child.getId());
    }
    parameters.put("childrenIds", childrenIds);
    return parameters;
  }

  @Override
  public List<JournalEntry> getEntries() {
    List<JournalEntry> entries = Lists.newArrayListWithCapacity(mChildren.size());
    for (Inode child : mChildren) {
      entries.add(child.toJournalEntry());
    }
    return entries;
  }
}
