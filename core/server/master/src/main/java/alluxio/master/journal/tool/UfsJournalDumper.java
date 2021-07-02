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

package alluxio.master.journal.tool;

import alluxio.AlluxioURI;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Implementation of {@link AbstractJournalDumper} for UFS journals.
 */
public class UfsJournalDumper extends AbstractJournalDumper {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalDumper.class);

  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);

  /**
   * Creates UFS journal dumper.
   *
   * @param master journal master
   * @param start journal start sequence number
   * @param end journal end sequence number
   * @param outputDir output dir for journal dump
   * @param inputDir input dir for journal files
   */
  public UfsJournalDumper(String master, long start, long end, String outputDir, String inputDir)
      throws IOException {
    super(master, start, end, outputDir, inputDir);
  }

  @Override
  public void dumpJournal() throws Throwable {
    try (
        UfsJournal journal = new UfsJournalSystem(getJournalLocation(mInputDir), 0)
            .createJournal(new NoopMaster(mMaster));
        PrintStream out =
            new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)));
        JournalReader reader = new UfsJournalReader(journal, mStart, true)) {
      boolean done = false;
      while (!done && reader.getNextSequenceNumber() < mEnd) {
        JournalReader.State state = reader.advance();
        switch (state) {
          case CHECKPOINT:
            try (CheckpointInputStream checkpoint = reader.getCheckpoint()) {
              Path dir = Paths.get(mCheckpointsDir + "-" + reader.getNextSequenceNumber());
              Files.createDirectories(dir);
              readCheckpoint(checkpoint, dir);
            }
            break;
          case LOG:
            Journal.JournalEntry entry = reader.getEntry();
            out.println(ENTRY_SEPARATOR);
            out.print(entry);
            break;
          case DONE:
            done = true;
            break;
          default:
            throw new RuntimeException("Unknown state: " + state);
        }
      }
    }
  }

  /**
   * @return the journal location
   */
  private URI getJournalLocation(String inputDir) {
    if (!inputDir.endsWith(AlluxioURI.SEPARATOR)) {
      inputDir += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(inputDir);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
