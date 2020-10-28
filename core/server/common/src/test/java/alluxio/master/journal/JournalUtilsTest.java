/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import static org.junit.Assert.assertEquals;

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link JournalUtils}.
 */
public final class JournalUtilsTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void checkpointAndRestore() throws IOException, InterruptedException {
    Journaled journaled = new TestJournaled(0);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JournalUtils.writeJournalEntryCheckpoint(baos, journaled);
    JournalUtils.restoreJournalEntryCheckpoint(
        new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())), journaled);
  }

  @Test
  public void restoreInvalidJournalEntryCheckpoint() throws IOException {
    Journaled journaled = new TestJournaled(0);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // Checkpoint version doesn't match Constants.JOURNAL_ENTRY_CHECKPOINT_VERSION.
    CheckpointOutputStream cos = new CheckpointOutputStream(baos, CheckpointType.COMPOUND);
    cos.flush();
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("Unrecognized checkpoint type");
    JournalUtils.restoreJournalEntryCheckpoint(
        new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())), journaled);
  }

  @Test
  public void checkpointAndRestoreComponents() throws Exception {
    List<TestJournaled> components = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      components.add(new TestJournaled(i));
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JournalUtils.writeToCheckpoint(baos, components);
    components.forEach(c -> assertEquals(0, c.getNumEntriesProcessed()));
    JournalUtils.restoreFromCheckpoint(
        new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())), components);
    components.forEach(c -> assertEquals(1, c.getNumEntriesProcessed()));
  }

  private static class TestJournaled implements Journaled {
    private final CheckpointName mName;
    private int mNumEntriesProcessed;

    public TestJournaled(int i) {
      mName = CheckpointName.values()[i];
    }

    @Override
    public boolean processJournalEntry(JournalEntry entry) {
      mNumEntriesProcessed++;
      return true;
    }

    public int getNumEntriesProcessed() {
      return mNumEntriesProcessed;
    }

    @Override
    public void resetState() {
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(Arrays.asList(JournalEntry.newBuilder()
          .setAddMountPoint(AddMountPointEntry.getDefaultInstance()).build()).iterator());
    }

    @Override
    public CheckpointName getCheckpointName() {
      return mName;
    }
  }
}
