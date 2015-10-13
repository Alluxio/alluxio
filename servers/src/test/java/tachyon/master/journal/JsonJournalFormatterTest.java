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

package tachyon.master.journal;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonJournalFormatterTest extends JournalFormatterTestBase {

  public static final String JSON_SAMPLE_PATH = "/JournalEntries.json";

  private static JsonNode sRootNode;

  @Override
  protected JournalFormatter getFormatter() {
    return new JsonJournalFormatter();
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    String entriesFile =
        JsonJournalFormatterTest.class.getResource(JSON_SAMPLE_PATH).getFile();
    sRootNode = new ObjectMapper().readTree(new File(entriesFile));
  }

  // given an entry type, it checks if a given json is deserialized properly
  private final void entryJsonTest(JournalEntryType type) throws IOException {
    JsonNode n = sRootNode.get(type.toString());
    new ObjectMapper().writeValue(mOs, n);
    JournalEntry readEntry = read();
    assertSameEntry(mDataSet.get(type), readEntry);
  }

  // Block

  @Test
  public void blockIdGeneratorEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.BLOCK_CONTAINER_ID_GENERATOR);
  }

  @Test
  public void blockInfoEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.BLOCK_INFO);
  }

  // FileSystem

  @Test
  public void inodeFileEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.INODE_FILE);
  }

  @Test
  public void inodeDirectoryEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.INODE_DIRECTORY);
  }

  @Test
  public void inodeLastModificationTimeEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.INODE_MTIME);
  }

  @Test
  public void persistedDirectoryEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.INODE_PERSISTED);
  }

  @Test
  public void persistFileEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.ADD_CHECKPOINT);
  }

  @Test
  public void completeFileEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.COMPLETE_FILE);
  }

  @Test
  public void setPinnedEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.SET_PINNED);
  }

  @Test
  public void deleteFileEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.DELETE_FILE);
  }

  @Test
  public void renameEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.RENAME);
  }

  @Test
  public void inodeDirectoryIdGeneratorEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.INODE_DIRECTORY_ID_GENERATOR);
  }

  @Test
  public void addMountPointEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.ADD_MOUNTPOINT);
  }

  @Test
  public void deleteMountPointEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.DELETE_MOUNTPOINT);
  }

  // RawTable

  @Test
  public void rawTableEntryJsonTest() throws IOException {
    entryJsonTest(JournalEntryType.RAW_TABLE);
  }

}
