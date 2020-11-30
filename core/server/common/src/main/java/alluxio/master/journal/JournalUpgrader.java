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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.MasterFactory;
import alluxio.master.NoopMaster;
import alluxio.master.ServiceUtils;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.ConfigurationUtils;
import alluxio.util.URIUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tool to upgrade journal from v0 to v1. Format the v1 journal before running this tool.
 *
 * It is strongly recommended to backup the v0 journal before running this tool to avoid losing
 * any data in case of failures.
 *
 * <pre>
 * java -cp \
 *   assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
 *   alluxio.master.journal.JournalUpgrader -journalDirectoryV0 YourJournalDirectoryV0
 * </pre>
 */
@NotThreadSafe
public final class JournalUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(JournalUpgrader.class);

  private static final int EXIT_FAILED = -1;
  private static final int EXIT_SUCCEEDED = 0;
  private static final Options OPTIONS =
      new Options().addOption("help", false, "Show help for this test.")
          .addOption("journalDirectoryV0", true,
              "Where the v0 journal is persisted. It is assumed to be the same as the v1 journal "
                  + "directory if not set.");

  private static boolean sHelp;
  private static String sJournalDirectoryV0;

  /**
   * A class that provides a way to upgrade the journal from v0 to v1.
   */
  private static final class Upgrader {
    private final String mMaster;
    private final alluxio.master.journalv0.MutableJournal mJournalV0;
    private final UfsJournal mJournalV1;

    private final UnderFileSystem mUfs;

    private final URI mCheckpointV0;
    private final URI mCompletedLogsV0;

    private final URI mCheckpointsV1;
    private final URI mLogsV1;

    private final AlluxioConfiguration mAlluxioConf;

    private Upgrader(String master, AlluxioConfiguration alluxioConf) {
      mMaster = master;
      mAlluxioConf = alluxioConf;
      mJournalV0 = (new alluxio.master.journalv0.MutableJournal.Factory(
          getJournalLocation(sJournalDirectoryV0))).create(master);
      mJournalV1 =
          new UfsJournal(getJournalLocation(ServerConfiguration
              .get(PropertyKey.MASTER_JOURNAL_FOLDER)), new NoopMaster(master), 0,
              Collections::emptySet);

      mUfs = UnderFileSystem.Factory.create(sJournalDirectoryV0,
          UnderFileSystemConfiguration.defaults(alluxioConf));

      mCheckpointV0 = URIUtils.appendPathOrDie(mJournalV0.getLocation(), "checkpoint.data");
      mCompletedLogsV0 = URIUtils.appendPathOrDie(mJournalV0.getLocation(), "completed");

      mCheckpointsV1 = URIUtils.appendPathOrDie(mJournalV1.getLocation(), "checkpoints");
      mLogsV1 = URIUtils.appendPathOrDie(mJournalV1.getLocation(), "logs");
    }

    /**
     * Upgrades journal from v0 to v1.
     */
    void upgrade() throws IOException {
      if (!mUfs.exists(mCheckpointV0.toString())) {
        LOG.info("No checkpoint is found for {}. No upgrade is required.", mMaster);
        return;
      }
      prepare();

      LOG.info("Starting to upgrade {} journal.", mMaster);
      boolean checkpointUpgraded = false;
      int logNumber = 1;
      URI completedLog;
      while (mUfs.exists((completedLog = getCompletedLogV0(logNumber)).toString())) {
        long start = -1;
        long end = -1;
        logNumber++;
        try (JournalFileParser parser = JournalFileParser.Factory.create(completedLog)) {
          alluxio.proto.journal.Journal.JournalEntry entry;
          while ((entry = parser.next()) != null) {
            if (start == -1) {
              start = entry.getSequenceNumber();
            }
            end = entry.getSequenceNumber();
          }
        }

        if (!checkpointUpgraded) {
          renameCheckpoint(start);
          checkpointUpgraded = true;
        }

        URI dst = URIUtils.appendPathOrDie(mLogsV1, String.format("0x%x-0x%x", start, end + 1));
        if (!mUfs.renameFile(completedLog.toString(), dst.toString()) && !mUfs
            .exists(dst.toString())) {
          throw new IOException(
              String.format("Failed to rename %s to %s.", completedLog.toString(), dst.toString()));
        }
      }

      if (!checkpointUpgraded) {
        renameCheckpoint(1);
      }

      LOG.info("Finished upgrading {} journal.", mMaster);
    }

    /**
     * Prepares journal writer to upgrade journals from v0 to v1.
     */
    private void prepare() throws IOException {
      alluxio.master.journalv0.JournalWriter journalWriterV0 = mJournalV0.getWriter();
      journalWriterV0.recover();
      journalWriterV0.completeLogs();
      journalWriterV0.close();

      if (!mJournalV1.isFormatted()) {
        LOG.info("Starting to format journal {}.", mJournalV1.getLocation());
        mJournalV1.format();
        LOG.info("Finished formatting journal {}.", mJournalV1.getLocation());
      }

      if (!mUfs.exists(mCheckpointsV1.toString())) {
        mUfs.mkdirs(mCheckpointsV1.toString(), MkdirsOptions.defaults(mAlluxioConf)
            .setCreateParent(true));
      }
      if (!mUfs.exists(mLogsV1.toString())) {
        mUfs.mkdirs(mLogsV1.toString(), MkdirsOptions.defaults(mAlluxioConf).setCreateParent(true));
      }
    }

    /**
     * Renames checkpoint.
     *
     * @param sequenceNumber the sequence number
     */
    private void renameCheckpoint(long sequenceNumber) throws IOException {
      URI dst = URIUtils.appendPathOrDie(mCheckpointsV1, String.format("0x0-0x%x", sequenceNumber));
      if (!mUfs.renameFile(mCheckpointV0.toString(), dst.toString()) && !mUfs
          .exists(dst.toString())) {
        throw new IOException(
            String.format("Failed to rename %s to %s.", mCheckpointV0.toString(), dst.toString()));
      }
    }

    /**
     * @param logNumber the log number to get the path for
     * @return the location of the completed log for a particular log number
     */
    private URI getCompletedLogV0(long logNumber) {
      return URIUtils
          .appendPathOrDie(mCompletedLogsV0, String.format("%s.%020d", "log", logNumber));
    }

    /**
     * @return the journal location
     */
    private URI getJournalLocation(String journalDirectory) {
      if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
        journalDirectory += AlluxioURI.SEPARATOR;
      }
      try {
        return new URI(journalDirectory);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private JournalUpgrader() {}  // prevent instantiation

  /**
   * Reads a journal via
   * {@code java -cp \
   * assembly/server/target/alluxio-assembly-server-<ALLUXIO-VERSION>-jar-with-dependencies.jar \
   * alluxio.master.journal.JournalUpgrader -master BlockMaster}.
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

    List<String> masters = new ArrayList<>();
    for (MasterFactory factory : ServiceUtils.getMasterServiceLoader()) {
      masters.add(factory.getName());
    }

    for (String master : masters) {
      Upgrader upgrader = new Upgrader(master,
          new InstancedConfiguration(ConfigurationUtils.defaults()));
      try {
        upgrader.upgrade();
      } catch (IOException e) {
        LOG.error("Failed to upgrade the journal for {}.", master, e);
        System.exit(EXIT_FAILED);
      }
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
    sJournalDirectoryV0 = cmd.getOptionValue("journalDirectoryV0",
        ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER));
    return true;
  }

  /**
   * Prints the usage.
   */
  private static void usage() {
    new HelpFormatter().printHelp("java -cp alluxio-" + RuntimeConstants.VERSION
            + "-jar-with-dependencies.jar alluxio.master.journal.JournalUpgrader",
        "Upgrades journal from v0 to v1", OPTIONS, "", true);
  }
}
