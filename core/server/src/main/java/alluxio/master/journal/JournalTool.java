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

package alluxio.master.journal;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import alluxio.Version;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

/**
 * Tool for reading the journal. It reads binary journal entries and prints human-readable ones to
 * standard out. Example usage below.
 *
 * <pre>
 * java -cp assembly/target/alluxio-assemblies-0.9.0-SNAPSHOT-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool < journal/FileSystemMaster/log.out
 * </pre>
 */
@NotThreadSafe
public final class JournalTool {
  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);
  /** Amount of time to wait before giving up on the user supplying a journal log via stdin. */
  private static final long TIMEOUT_MS = 2000;
  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = -1;
  private static final Options OPTIONS = new Options()
      .addOption("help", false, "Show help for this test")
      .addOption("noTimeout", false, "Wait indefinitely for stdin to supply input");

  private static boolean sNoTimeout = false;
  private static boolean sHelp = false;

  /**
   * Reads a journal via
   * {@code java -cp \
   * assembly/target/alluxio-assemblies-<VERSION-OF-TACHYON>-jar-with-dependencies.jar \
   * alluxio.master.journal.JournalTool < journal/FileSystemMaster/log.out}.
   *
   * @param args arguments passed to the tool
   * @throws IOException if a non-Tachyon related exception occurs
   */
  public static void main(String[] args) throws IOException {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(EXIT_FAILED);
    }
    if (sHelp) {
      usage();
      System.exit(EXIT_SUCCEEDED);
    }
    if (!sNoTimeout && !stdinHasData()) {
      System.exit(EXIT_FAILED);
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
   * Parses the input args with a command line format, using
   * {@link org.apache.commons.cli.CommandLineParser}.
   *
   * @param args the input args
   * @return true if parsing succeeded
   */
  private static boolean parseInputArgs(String[] args) {
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sNoTimeout = cmd.hasOption("noTimeout");
    sHelp = cmd.hasOption("help");
    return true;
  }

  /**
   * @return true if stdin has data before {@link #TIMEOUT_MS} elapses
   */
  private static boolean stdinHasData() throws IOException {
    long start = System.currentTimeMillis();
    while (System.in.available() == 0) {
      if (System.currentTimeMillis() - start > TIMEOUT_MS) {
        System.out.println(
            "Timed out waiting for input from stdin. Use -noTimeout to wait longer.");
        return false;
      }
      CommonUtils.sleepMs(50);
    }
    return true;
  }

  private static void usage() {
    new HelpFormatter().printHelp(
        "java -cp alluxio-" + Version.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read a alluxio journal from stdin and write it human-readably to stdout", OPTIONS, "",
        true);
  }
}
