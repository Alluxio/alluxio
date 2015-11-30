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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.util.CommonUtils;

/**
 * Tool for reading the journal. It reads binary journal entries and prints human-readable ones to
 * standard out. Example usage below.
 *
 * <pre>
 * java -cp assembly/target/tachyon-assemblies-0.9.0-SNAPSHOT-jar-with-dependencies.jar \
 *   tachyon.master.journal.JournalTool < journal/FileSystemMaster/log.out
 * </pre>
 */
public final class JournalTool {
  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);
  /** Amount of time to wait before giving up on the user supplying a journal log via stdin. */
  private static final long TIMEOUT_MS =
      new TachyonConf().getLong(Constants.MASTER_JOURNAL_TOOL_STDIN_TIMEOUT_MS);

  public static void main(String[] args) throws FileNotFoundException, IOException {
    if (!(args.length == 0 && stdinHasData())) {
      usage();
      System.exit(-1);
    }

    JournalFormatter formatter = new ProtoBufJournalFormatter();
    JournalInputStream journalStream = formatter.deserialize(System.in);
    JournalEntry entry;
    while ((entry = journalStream.getNextEntry()) != null) {
      System.out.print(entry);
      System.out.println(ENTRY_SEPARATOR);
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
      CommonUtils.sleepMs(50);
    }
    return true;
  }

  private static void usage() {
    System.out.println("JournalTool < /path/to/journal");
    System.out.println("            The journal file should be provided through stdin.");
  }
}
