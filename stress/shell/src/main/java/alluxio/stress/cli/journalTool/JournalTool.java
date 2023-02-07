package alluxio.stress.cli.journalTool;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry ;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

public class JournalTool {

  private static final String HELP_OPTION_NAME = "help";
  private static final String MASTER_OPTION_NAME = "master";
  private static final String START_OPTION_NAME = "start";
  private static final String END_OPTION_NAME = "end";
  private static final String INPUT_DIR_OPTION_NAME = "inputDir";
  private static final String OUTPUT_DIR_OPTION_NAME = "outputDir";

  private static final Options OPTIONS = new Options()
      .addOption(HELP_OPTION_NAME, false, "Show help for this command.")
      .addOption(MASTER_OPTION_NAME, true,
          "The name of the master (e.g. FileSystemMaster, BlockMaster). "
              + "Set to FileSystemMaster by default.")
      .addOption(START_OPTION_NAME, true,
          "The start log sequence number (inclusive). Set to 0 by default.")
      .addOption(END_OPTION_NAME, true,
          "The end log sequence number (exclusive). Set to +inf by default.")
      .addOption(INPUT_DIR_OPTION_NAME, true,
          "The input directory on-disk to read journal content from. "
              + "(Default: Read from system configuration.)")
      .addOption(OUTPUT_DIR_OPTION_NAME, true,
          "The output directory to write journal content to. "
          + "(Default: journal_dump-${timestamp})");

  private static boolean sHelp;
  private static String sMaster;
  private static String sInputDir;
  private static long sStart;
  private static long sEnd;
  private static String sOutputDir;

  public static void main(String[] args) {
    if (!parseInputArgs(args)) {
      System.exit(-1);
    }
    if (sHelp) {
      System.exit(0);
    }

    EntryStream stream = initStream();
    JournalEntry entry = stream.nextEntry();
    JournalEntry hold = stream.nextEntry();
    long sq = hold.getSequenceNumber();
    long step = 5;
    hold.toBuilder().setSequenceNumber(sq + step);
  }

   private static boolean parseInputArgs(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      System.out.println("Failed to parse input args: " + e);
      return false;
    }
    sHelp = cmd.hasOption(HELP_OPTION_NAME);
    sMaster = cmd.getOptionValue(MASTER_OPTION_NAME, "FileSystemMaster");
    sStart = Long.decode(cmd.getOptionValue(START_OPTION_NAME, "0"));
    sEnd = Long.decode(cmd.getOptionValue(END_OPTION_NAME, Long.valueOf(Long.MAX_VALUE)
        .toString()));
    if (cmd.hasOption(INPUT_DIR_OPTION_NAME)) {
      sInputDir = new File(cmd.getOptionValue(INPUT_DIR_OPTION_NAME)).getAbsolutePath();
    } else {
      sInputDir = Configuration.getString(PropertyKey.MASTER_JOURNAL_FOLDER);
    }
    sOutputDir = new File(cmd.getOptionValue(OUTPUT_DIR_OPTION_NAME,
        "journal_dump-" + System.currentTimeMillis())).getAbsolutePath();
    return true;
  }

  private static EntryStream initStream() {
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    switch (journalType) {
      case UFS:
        return new UfsJournalEntryStream(sMaster, sStart, sEnd, sInputDir);
      case EMBEDDED:
        return new RaftJournalEntryStream(sMaster, sStart, sEnd, sInputDir);
      default:
        System.err.printf("Unsupported journal type: %s%n", journalType.name());
    }
    throw new RuntimeException();
  }

  private static JournalWriter initJournal() {
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    switch (journalType) {
      case UFS:
      case EMBEDDED:
      default:
    }
    return null;
  }

}