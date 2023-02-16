package alluxio.stress.cli.journaldisruptor;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.JournalClosedException;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;
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

/**
 * JournalTool (Journal Disruptor).
 */
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

  /**
   * the main method.
   * @param args
   */
  public static void main(String[] args) {
    if (!parseInputArgs(args)) {
      System.exit(-1);
    }
    if (sHelp) {
      System.exit(0);
    }

    disrupttest();
    System.exit(0);
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
    sInputDir = "/Users/dengxinyu/alluxio-2.8.0/tmp/journal";
    sOutputDir = new File(cmd.getOptionValue(OUTPUT_DIR_OPTION_NAME,
        "journal_dump-" + System.currentTimeMillis())).getAbsolutePath();
    sOutputDir = "/Users/dengxinyu/journal-tool";
    return true;
  }

  private static JournalWriter initJournal() throws IOException {
    JournalExporter ex = new JournalExporter(sInputDir, sMaster, sStart);
    return ex.getWriter();
  }

  /**
   * test pass!
   * read 10 entries from 0x0-0x21, decode, encode, and write them to another journal file
   */
  public static void disrupttest() {
    JournalExporter ex;
    JournalWriter writer;
    JournalReader reader = new JournalReader(sMaster, sStart, sEnd, sInputDir);
    JournalDisruptor disruptor = new JournalDisruptor(reader, 3, 6);
    String outputfile = PathUtils.concatPath(sOutputDir, "test.txt");
    try (PrintStream out = new PrintStream(new BufferedOutputStream(
        new FileOutputStream(outputfile)))) {
      ex = new JournalExporter(sOutputDir, sMaster, sStart);
      writer = ex.getWriter();
      // this loop is use used to go through the journal entries
      // here read 22 alluxio journal entries
      for (int i = 0; i < 23; i++) {
        JournalEntry entry = disruptor.test();
        if (entry != null) {
          System.out.println("entry: " + entry);
          try {
            writer.write(entry.toBuilder().clearSequenceNumber().build());
            out.println(entry.toBuilder().clearSequenceNumber().build());
          } catch (JournalClosedException e) {
            System.out.println("failed when writing entry: " + e);
          }
        } else {
          break;
        }
      }
      try {
        writer.flush();
      } catch (Exception e) {
        System.out.println(e);
      }
      // without this sleep RaftJournalClient won't be Master
      // 'is in LEADER state but not ready yet'
      Thread.sleep(2000);
      writer.close();
      ex.getJournal().close();
      out.flush();
    } catch (IOException e) {
      System.out.println(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * the actually main action.
   */
  public static void mainInternal() {
    JournalExporter ex;
    JournalWriter writer;
    JournalReader reader = new JournalReader(sMaster, sStart, sEnd, sInputDir);
    JournalDisruptor disruptor = new JournalDisruptor(reader, 3, 6);
    String outputfile = PathUtils.concatPath(sOutputDir, "test.txt");

    try (PrintStream out = new PrintStream(new BufferedOutputStream(
        new FileOutputStream(outputfile)))) {
      ex = new JournalExporter(sOutputDir, sMaster, sStart);
      writer = ex.getWriter();
      JournalEntry entry;
      // this means process all journal
      while ((entry = disruptor.nextEntry()) != null) {
        System.out.println("entry: " + entry);
        try {
          writer.write(entry.toBuilder().clearSequenceNumber().build());
          out.println(entry.toBuilder().clearSequenceNumber().build());
        } catch (JournalClosedException e) {
          System.out.println("failed when writing entry: " + e);
        }
      }
      try {
        writer.flush();
      } catch (Exception e) {
        System.out.println(e);
      }
      Thread.sleep(1000);
      writer.close();
      ex.getJournal().close();
      out.flush();
    } catch (IOException e) {
      System.out.println(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
