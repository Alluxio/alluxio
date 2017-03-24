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

package alluxio.master.journal.ufs;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.master.journal.ufs.UfsJournalWriter.EntryOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Unit tests for {@link UfsJournalWriter}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(UfsJournalWriter.class)
public class UfsJournalWriterTest {

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;

  @Before
  public void before() throws Exception {
    File journalFolder = mFolder.newFolder();
    mJournal = new UfsMutableJournal(new URI(journalFolder.getAbsolutePath()));
  }

  @Test
  public void rotateLogOnFlushIOException() throws Exception {
    // Setup so that we can trigger an IOException when flush is called on the underlying stream.
    UfsJournalWriter mockJournalWriter = PowerMockito.mock(UfsJournalWriter.class);
    OutputStream mockOutStream = mock(OutputStream.class);
    UnderFileSystem mockUfs = mock(UnderFileSystem.class);
    doReturn(mockOutStream).when(mockUfs).create(eq(mJournal.getCurrentLog().toString()),
        any(CreateOptions.class));

    EntryOutputStream entryOutStream = new EntryOutputStream(mockUfs,
        mJournal.getCurrentLog(), mJournal.getJournalFormatter(), mockJournalWriter);
    entryOutStream.write(JournalEntry.newBuilder().build());
    doThrow(new IOException("flush failed")).when(mockOutStream).flush();
    try {
      entryOutStream.flush();
      Assert.fail("Should have thrown an exception");
    } catch (IOException ignored) {
      // expected
    }
    // Undo the earlier mocking so that the write doesn't fail.
    doNothing().when(mockOutStream).flush();
    // The rotation happens the next time an entry is written.
    entryOutStream.write(JournalEntry.newBuilder().build());
    verify(mockJournalWriter).completeCurrentLog();
  }

  @Test
  public void rotateLogOnSyncException() throws Exception {
    // Setup so that we can trigger an IOException when sync is called on the underlying stream.
    UfsJournalWriter mockJournalWriter = PowerMockito.mock(UfsJournalWriter.class);
    FSDataOutputStream mockOutStream = mock(FSDataOutputStream.class);
    UnderFileSystem mockUfs = mock(UnderFileSystem.class);
    doReturn(mockOutStream).when(mockUfs).create(eq(mJournal.getCurrentLog().toString()),
        any(CreateOptions.class));

    EntryOutputStream entryOutStream = new EntryOutputStream(mockUfs,
        mJournal.getCurrentLog(), mJournal.getJournalFormatter(), mockJournalWriter);
    entryOutStream.write(JournalEntry.newBuilder().build());
    doThrow(new IOException("sync failed")).when(mockOutStream).sync();
    try {
      entryOutStream.flush();
      Assert.fail("Should have thrown an exception");
    } catch (IOException ignored) {
      // expected
    }
    // Undo the earlier mocking so that the write doesn't fail.
    doNothing().when(mockOutStream).sync();
    // The rotation happens the next time an entry is written.
    entryOutStream.write(JournalEntry.newBuilder().build());
    verify(mockJournalWriter).completeCurrentLog();
  }

  @Test
  public void rotateLogOnWriteException() throws Exception {
    // Setup so that we can trigger an IOException when a write is performed on the underlying
    // stream.
    UfsJournalWriter mockJournalWriter = PowerMockito.mock(UfsJournalWriter.class);
    FSDataOutputStream mockOutStream = mock(FSDataOutputStream.class);
    UnderFileSystem mockUfs = mock(UnderFileSystem.class);
    doReturn(mockOutStream).when(mockUfs).create(eq(mJournal.getCurrentLog().toString()),
        any(CreateOptions.class));

    EntryOutputStream entryOutStream = new EntryOutputStream(mockUfs,
        mJournal.getCurrentLog(), mJournal.getJournalFormatter(), mockJournalWriter);
    doThrow(new IOException("write failed")).when(mockOutStream).write(any(byte[].class), anyInt(),
        anyInt());
    try {
      entryOutStream.write(JournalEntry.newBuilder().setSequenceNumber(10).build());
      Assert.fail("Should have thrown an exception");
    } catch (IOException ignored) {
      // expected
    }
    // Undo the earlier mocking so that the write doesn't fail.
    doNothing().when(mockOutStream).write(any(byte[].class), anyInt(), anyInt());
    // The rotation happens the next time an entry is written.
    entryOutStream.write(JournalEntry.newBuilder().build());

    verify(mockJournalWriter).completeCurrentLog();
  }
}
