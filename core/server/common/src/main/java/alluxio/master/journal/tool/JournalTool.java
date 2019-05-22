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

import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool for converting journal to a human-readable format.
 *
 * <pre>
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalTool -master FileSystemMaster -outputDir my-journal
 * </pre>
 */
@NotThreadSafe
public final class JournalTool {
  private static final Logger LOG = LoggerFactory.getLogger(JournalTool.class);
  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS = new Options()
      .addOption("help", false, "Show help for this command.")
      .addOption("master", true,
          "The name of the master (e.g. FileSystemMaster, BlockMaster). "
              + "Set to FileSystemMaster by default.")
      .addOption("start", true,
          "The start log sequence number (inclusive). Set to 0 by default.")
      .addOption("end", true,
          "The end log sequence number (exclusive). Set to +inf by default.")
      .addOption("inputDir", true,
          "The input directory on-disk to read journal content from. "
              + "(Default: Read from system configuration.)")
      .addOption("outputDir", true,
          "The output directory to write journal content to. "
          + "(Default: journal_dump-${timestamp})");

  private static boolean sHelp;
  private static String sMaster;
  private static long sStart;
  private static long sEnd;
  private static String sInputDir;
  private static String sOutputDir;

  private JournalTool() {} // prevent instantiation

  /**
   * Dumps a ufs journal in human-readable format.
   *
   * @param args arguments passed to the tool
   */
  public static void main(String[] args) throws Throwable {
    if (!parseInputArgs(args)) {
      usage();
      System.exit(EXIT_FAILED);
    }
    if (sHelp) {
      usage();
      System.exit(EXIT_SUCCEEDED);
    }

    try {
      dumpJournal();
    } catch (Exception exc) {
      System.out.println(String.format("Journal tool failed: %s", exc));
    }
  }

  @SuppressFBWarnings(value = "DB_DUPLICATE_SWITCH_CLAUSES")
  private static void dumpJournal() throws Throwable {
    JournalType journalType =
        ServerConfiguration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);

    AbstractJournalDumper journalDumper;
    switch (journalType) {
      case UFS:
        journalDumper = new UfsJournalDumper(sMaster, sStart, sEnd, sOutputDir, sInputDir);
        break;
      case EMBEDDED:
        journalDumper = new RaftJournalDumper(sMaster, sStart, sEnd, sOutputDir, sInputDir);
        break;
      default:
        System.err.println(String.format("Unsupported journal type: %s", journalType.name()));
        return;
    }

    System.out.println(
        String.format("Dumping journal of type %s to %s", journalType.name(), sOutputDir));
    journalDumper.dumpJournal();
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
    sStart = Long.decode(cmd.getOptionValue("start", "0"));
    sEnd = Long.decode(cmd.getOptionValue("end", Long.valueOf(Long.MAX_VALUE).toString()));
    sInputDir = new File(
        cmd.getOptionValue("inputDir", ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER)))
            .getAbsolutePath();
    sOutputDir =
        new File(cmd.getOptionValue("outputDir", "journal_dump-" + System.currentTimeMillis()))
            .getAbsolutePath();
    return true;
  }

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp(
        "java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalTool",
        "Read an Alluxio journal and write it to a directory in a human-readable format.", OPTIONS,
        "", true);
  }
}
