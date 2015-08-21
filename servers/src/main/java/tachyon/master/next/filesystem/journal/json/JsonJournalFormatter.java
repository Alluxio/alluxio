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

package tachyon.master.next.filesystem.journal.json;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.master.next.filesystem.journal.AddCheckpointEntry;
import tachyon.master.next.filesystem.journal.InodeFileEntry;
import tachyon.master.next.filesystem.journal.JournalFormatter;
import tachyon.master.next.journal.JournalEntryType;

public class JsonJournalFormatter extends JournalFormatter {
  private ObjectWriter mWriter = JsonObject.createObjectMapper().writer();
  private long mEventId = 0;

  @Override
  protected void serializeAddCheckpointEntry(AddCheckpointEntry entry, OutputStream os)
      throws IOException {
    EventEntry event =
        new EventEntry(JournalEntryType.ADD_CHECKPOINT, ++mEventId)
            .withParameter("fileId", entry.fileId()).withParameter("length", entry.length())
            .withParameter("path", entry.checkpointPath().toString())
            .withParameter("opTimeMs", entry.opTimeMs());

    writeEntry(event, os);
  }

  @Override
  protected void serializeInodeFileEntry(InodeFileEntry entry, OutputStream os) throws IOException {
    CheckpointEntry checkpoint =
        new CheckpointEntry(JournalEntryType.INODE_FILE)
            .withParameter("creationTimeMs", entry.creationTimeMs())
            .withParameter("id", entry.id()).withParameter("name", entry.name())
            .withParameter("parentId", entry.parentId())
            .withParameter("blockSizeBytes", entry.blockSizeBytes())
            .withParameter("length", entry.length()).withParameter("complete", entry.isComplete())
            .withParameter("pin", entry.isPinned()).withParameter("cache", entry.isCache())
            .withParameter("ufsPath", entry.ufsPath())
            .withParameter("lastModificationTimeMs", entry.lastModificationTimeMs());

    writeEntry(checkpoint, os);
  }

  private void writeEntry(JsonObject entry, OutputStream os) throws IOException {
    mWriter.writeValue(os, entry);
    (new DataOutputStream(os)).writeByte('\n');
  }
}
