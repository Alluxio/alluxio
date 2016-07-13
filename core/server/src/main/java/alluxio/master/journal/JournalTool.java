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

import alluxio.RuntimeConstants;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

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
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS = new Options()
      .addOption("help", false, "Show help for this test")
      .addOption("noTimeout", false, "Wait indefinitely for stdin to supply input");

  private static boolean sNoTimeout = false;
  private static boolean sHelp = false;

  private JournalTool() {} // prevent instantiation

  /**
   * Reads a journal via
   * {@code java -cp \
   * assembly/target/alluxio-assemblies-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
   * alluxio.master.journal.JournalTool < journal/FileSystemMaster/log.out}.
   *
   * @param args arguments passed to the tool
   * @throws IOException if a non-Alluxio related exception occurs
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
    CommandLine cmd;
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
        "java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read an Alluxio journal from stdin and write it human-readably to stdout", OPTIONS, "",
        true);
  }
}
