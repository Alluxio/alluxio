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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.proto.journal.Journal.JournalEntry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool for reading the journal entries given a range of sequence numbers. It reads binary journal
 * entries and prints human-readable ones to standard out. Example usage below.
 *
 * <pre>
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool -master FileSystemMaster -start 0x100 -end 0x109
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool -journalFile YourJournalFilePath
 * </pre>
 */
@NotThreadSafe
public final class JournalTool {
  private static final Logger LOG = LoggerFactory.getLogger(JournalTool.class);
  /** Separator to place at the end of each journal entry. */
  private static final String ENTRY_SEPARATOR = StringUtils.repeat('-', 80);
  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS =
      new Options()
          .addOption("help", false, "Show help for this test.")
          .addOption("master", true, "The name of the master (e.g. FileSystemMaster, BlockMaster). "
              + "Set to FileSystemMaster by default.")
          .addOption("start", true,
              "The start log sequence number (inclusive). Set to 0 by default.")
          .addOption("end", true,
              "The end log sequence number (exclusive). Set to +inf by default.")
          .addOption("journalFile", true,
              "If set, only read journal from this file. -master is ignored when -journalFile is "
                  + "set.");

  private static boolean sHelp;
  private static String sMaster;
  private static long sStart;
  private static long sEnd;
  private static String sJournalFile;

  private JournalTool() {} // prevent instantiation

  /**
   * Reads a journal via
   * {@code java -cp \
   * assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
   * alluxio.master.journal.JournalTool -master BlockMaster -start 0x100 -end 0x109}.
   *
   * @param args arguments passed to the tool
   */
  public static void main(String[] args) {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(EXIT_FAILED);
    }
    if (sHelp) {
      usage();
      System.exit(EXIT_SUCCEEDED);
    }

    if (sJournalFile != null && !sJournalFile.isEmpty()) {
      parseJournalFile();
    } else {
      readFromJournal();
    }
  }

  private static void parseJournalFile() {
    URI location;
    try {
      location = new URI(sJournalFile);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    try (JournalFileParser parser = JournalFileParser.Factory.create(location)) {
      JournalEntry entry;
      while ((entry = parser.next()) != null) {
        if (entry.getSequenceNumber() < sStart) {
          continue;
        }
        if (entry.getSequenceNumber() >= sEnd) {
          break;
        }
        System.out.println(ENTRY_SEPARATOR);
        System.out.print(entry);
      }
    } catch (Exception e) {
      LOG.error("Failed to get next journal entry.", e);
    }
  }

  private static void readFromJournal() {
    JournalFactory factory = new Journal.Factory(getJournalLocation());
    Journal journal = factory.create(sMaster);
    JournalReaderOptions options =
        JournalReaderOptions.defaults().setPrimary(true).setNextSequenceNumber(sStart);
    try (JournalReader reader = journal.getReader(options)) {
      JournalEntry entry;
      while ((entry = reader.read()) != null) {
        if (entry.getSequenceNumber() >= sEnd) {
          break;
        }
        System.out.println(ENTRY_SEPARATOR);
        System.out.print(entry);
      }
    } catch (Exception e) {
      LOG.error("Failed to read next journal entry.", e);
    }
  }

  /**
   * @return the journal location
   */
  private static URI getJournalLocation() {
    String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
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
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sHelp = cmd.hasOption("help");
    sMaster = cmd.getOptionValue("master", "FileSystemMaster");
    sStart = Long.parseLong(cmd.getOptionValue("start", "0"));
    sEnd = Long.parseLong(cmd.getOptionValue("end", Long.valueOf(Long.MAX_VALUE).toString()));
    sJournalFile = cmd.getOptionValue("journalFile", "");
    return true;
  }

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp("java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read an Alluxio journal and write it to stdout in a human-readable format.", OPTIONS, "",
        true);
  }
}
