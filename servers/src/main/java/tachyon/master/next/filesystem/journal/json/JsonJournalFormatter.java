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

import java.io.IOException;
import java.io.OutputStream;

import tachyon.master.next.filesystem.journal.AddCheckpointEntry;
import tachyon.master.next.filesystem.journal.InodeFileEntry;
import tachyon.master.next.filesystem.journal.JournalFormatter;

public class JsonJournalFormatter extends JournalFormatter {
  @Override
  protected void serializeAddCheckpointEntry(AddCheckpointEntry entry, OutputStream os) throws IOException {
    // TODO
  }

  @Override
  protected void serializeInodeFileEntry(InodeFileEntry entry, OutputStream os) throws IOException {
    // TODO
  }
}
