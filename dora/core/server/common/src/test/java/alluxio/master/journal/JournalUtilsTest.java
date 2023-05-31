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
import static org.junit.Assert.assertTrue;

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link JournalUtils}.
 */
public final class JournalUtilsTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

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

  @Test
  public void noEntryTest() throws IOException, InterruptedException {
    testEntries(0);
  }

  @Test
  public void oneEntryTest() throws IOException, InterruptedException {
    testEntries(1);
  }

  @Test
  public void multiEntryTest() throws IOException, InterruptedException {
    testEntries(5);
  }

  private void testEntries(int numEntries) throws IOException, InterruptedException {
    TestMultiEntryJournaled journaled = createJournaled(numEntries, 0L);
    ArrayList<JournalEntry> copy = new ArrayList<>(journaled.mProcessedEntries);
    File file = mFolder.newFile();
    try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
      JournalUtils.writeJournalEntryCheckpoint(outputStream, journaled);
    }
    journaled.resetState();
    try (CheckpointInputStream inputStream =
             new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      JournalUtils.restoreJournalEntryCheckpoint(inputStream, journaled);
    }
    Assert.assertEquals(copy, journaled.mProcessedEntries);
  }

  @Test
  public void testCompoundNone() throws IOException, InterruptedException {
    testCompound(0);
  }

  @Test
  public void testCompoundOne() throws IOException, InterruptedException {
    testCompound(1);
  }

  @Test
  public void testCompoundMulti() throws IOException, InterruptedException {
    testCompound(5);
  }

  private void testCompound(int numElements) throws IOException, InterruptedException {
    List<Checkpointed> checkpointed = new ArrayList<>(numElements);
    int numEntries = 5;
    long sequenceNumber = 0;
    for (int i = 0; i < numElements; i++) {
      if (i % 2 == 0) {
        checkpointed.add(createJournaled(numEntries, sequenceNumber));
      } else {
        checkpointed.add(new TestCheckpointed(numEntries, sequenceNumber));
      }
      sequenceNumber += numEntries;
    }

    ArrayList<Checkpointed> copy = new ArrayList<>(checkpointed);
    File file = mFolder.newFile();
    try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
      JournalUtils.writeToCheckpoint(outputStream, checkpointed);
    }
    for (Checkpointed c : checkpointed) {
      if (c instanceof Journaled) {
        ((Journaled) c).resetState();
      } else if (c instanceof TestCheckpointed) {
        ((TestCheckpointed) c).clear();
      }
    }
    try (CheckpointInputStream inputStream =
             new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      JournalUtils.restoreFromCheckpoint(inputStream, checkpointed);
    }
    assertEquals(copy, checkpointed);
  }

  private TestMultiEntryJournaled createJournaled(int numEntries, long baseSequenceNumber) {
    TestMultiEntryJournaled journaled = new TestMultiEntryJournaled();
    for (int i = 0; i < numEntries; i++) {
      journaled.processJournalEntry(
          JournalEntry.newBuilder().setSequenceNumber(baseSequenceNumber + i).build());
    }
    return journaled;
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

  private static class TestMultiEntryJournaled implements Journaled {
    private static int sIndex = 0;
    private final CheckpointName mName;
    private final List<JournalEntry> mProcessedEntries = new ArrayList<>();

    TestMultiEntryJournaled() {
      mName = CheckpointName.values()[sIndex];
      sIndex = (sIndex + 1);
      assertTrue("Cannot create too many Journaled instances",
          sIndex <= CheckpointName.values().length);
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(mProcessedEntries.iterator());
    }

    @Override
    public boolean processJournalEntry(JournalEntry entry) {
      return mProcessedEntries.add(entry);
    }

    @Override
    public void resetState() {
      mProcessedEntries.clear();
    }

    @Override
    public CheckpointName getCheckpointName() {
      return mName;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestMultiEntryJournaled
          && mProcessedEntries.equals(((TestMultiEntryJournaled) obj).mProcessedEntries);
    }
  }

  private static class TestCheckpointed implements Checkpointed {
    private static long sLong = 0L;
    private final CheckpointName mName;
    private final int mSize;
    private final List<Long> mState = new ArrayList<>();

    TestCheckpointed(int numLongs, long baseLong) {
      mName = new TestMultiEntryJournaled().getCheckpointName();
      mSize = numLongs;
      for (int i = 0; i < mSize; i++) {
        mState.add(baseLong + i);
      }
    }

    public void clear() {
      mState.clear();
    }

    @Override
    public CheckpointName getCheckpointName() {
      return mName;
    }

    @Override
    public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
      CheckpointOutputStream outputStream = new CheckpointOutputStream(output,
          CheckpointType.LONGS);
      for (Long l : mState) {
        outputStream.writeLong(l);
      }
    }

    @Override
    public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
      assertEquals(CheckpointType.LONGS, input.getType());
      for (int i = 0; i < mSize; i++) {
        long l = input.readLong();
        mState.add(l);
      }
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestCheckpointed
          && mState.equals(((TestCheckpointed) obj).mState);
    }
  }
}
