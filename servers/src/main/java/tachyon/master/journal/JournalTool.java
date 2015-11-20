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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;

import tachyon.proto.journal.Journal.JournalEntry;

/**
 * Tool for reading the journal. It reads binary journal entries and prints human-readable ones to
 * standard out. Example usage below.
 *
 * <pre>
 * java -cp assembly/target/tachyon-assemblies-0.9.0-SNAPSHOT-jar-with-dependencies.jar \
 *   tachyon.master.journal.JournalTool journal/FileSystemMaster/log.out
 * </pre>
 */
public final class JournalTool {
  /** Number of dashes to place on a newline at the end of each journal entry. */
  private static final int NUM_DASHES = 80;
  /** Amount of time to wait before giving up on the user supplying a journal log via stdin. */
  private static final long TIMEOUT_MS = 500;

  public static void main(String[] args) throws FileNotFoundException, IOException {
    InputStream inStream = null;
    if (args.length == 0 && stdinHasData()) {
      inStream = System.in;
    } else if (args.length == 1) {
      inStream = new FileInputStream(args[0]);
    } else {
      usage();
      System.exit(-1);
    }
    JournalFormatter formatter = new ProtoBufJournalFormatter();
    JournalInputStream journalStream = formatter.deserialize(inStream);
    JournalEntry entry;
    while ((entry = journalStream.getNextEntry()) != null) {
      System.out.print(entry);
      System.out.println(StringUtils.repeat('-', NUM_DASHES));
    }
  }

  /**
   * @return true if stdin has data before TIMEOUT_MS elapses
   */
  private static boolean stdinHasData() throws IOException {
    long start = System.currentTimeMillis();
    while (System.in.available() == 0) {
      if (System.currentTimeMillis() - start > TIMEOUT_MS) {
        return false;
      }
    }
    return true;
  }

  private static void usage() {
    System.out.println(
        "JournalTool [journal_file]\n"
      + "            The journal file to read may either be streamed through stdin, or\n"
      + "            provided on the command line.");
  }
}
