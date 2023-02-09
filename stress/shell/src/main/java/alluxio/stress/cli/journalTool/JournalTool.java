package alluxio.stress.cli.journalTool;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.JournalClosedException;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry ;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

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
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    switch (journalType) {
      case UFS:
        ufstest();
        break;
      case EMBEDDED:
        rafttest();
        break;
      default:
        System.out.println("no such type journal, no test shall be executed");
    }
    String testpath = "/Users/dengxinyu/test.txt";
    try (PrintStream test =
          new PrintStream(new BufferedOutputStream(new FileOutputStream(testpath)))) {
      test.println("hello");
    } catch (Exception e) {
      System.out.print(e);
    }
    // EntryStream stream = initStream();
    // JournalEntry entry = stream.nextEntry();
    // JournalEntry hold = stream.nextEntry();
    // long sq = hold.getSequenceNumber();
    // long step = 5;
    // hold.toBuilder().setSequenceNumber(sq + step);
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

  private static JournalWriter initJournal() throws IOException {
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    JournalExporter ex = new JournalExporter(journalType, sInputDir, sMaster, sStart);
    return ex.getWriter();
  }

  /**
   * test pass!
   * read 10 entries from 0x0-0x21, decode, encode, and write them to another journal file
   */
  private static void ufstest() {
    EntryStream stream = initStream();
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    JournalExporter ex;
    JournalWriter writer;
    String outputfile = PathUtils.concatPath(sOutputDir, "test.txt");
    try (PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputfile)))) {
      ex = new JournalExporter(journalType, sOutputDir, sMaster, sStart);
      Journal journal = ex.getJournal();
      writer = ex.getWriter();
      JournalContext ctx = journal.createJournalContext();
      for (int i = 0; i < 10; i++) {
        JournalEntry entry = stream.nextEntry();
        writer.write(entry);
      }
      writer.flush();
      ex.getJournal().close();
    } catch (IOException e) {
      System.out.print(e);
    } catch (JournalClosedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void rafttest() {
    EntryStream stream = initStream();
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    JournalExporter ex;
    JournalWriter writer;
    String outputfile = PathUtils.concatPath(sOutputDir, "test.txt");
    try (PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputfile)))) {
      ex = new JournalExporter(journalType, sOutputDir, sMaster, sStart);
      Journal journal = ex.getJournal();
      // writer = ex.getWriter();
      JournalContext ctx = journal.createJournalContext();
      for (int i = 0; i < 10; i++) {
        JournalEntry entry = stream.nextEntry();
        // writer.write(entry);
        out.println(entry);
      }
      // writer.flush();
      ex.getJournal().close();
    } catch (IOException e) {

    }
  }

}