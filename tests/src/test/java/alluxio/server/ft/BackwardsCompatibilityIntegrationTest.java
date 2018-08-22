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

package alluxio.server.ft;

import alluxio.AlluxioTestDirectory;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.backcompat.BackwardsCompatibilityJournalGenerator;
import alluxio.master.backcompat.Journal;
import alluxio.master.backcompat.TestOp;
import alluxio.multi.process.Clients;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.Builder;
import alluxio.multi.process.PortCoordination;
import alluxio.security.LoginUser;
import alluxio.testutils.BaseIntegrationTest;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests that the current version can read journals from previous versions, including the current
 * version.
 *
 * To cover many types of journal entries, the test runs a series of TestOps, each of which makes
 * some independent modification to master state, and has a method to verify that the change was
 * correctly applied. To add a new TestOp, implement the TestOp interface or extend FsTestOp, and
 * add your TestOp to the OPS list. Then either re-generate all journal artifacts from previous
 * versions (not easy at the moment, could add tooling for this later), or implement
 * TestOp#supportsVersion to only match the latest version and future versions.
 */
public final class BackwardsCompatibilityIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BackwardsCompatibilityIntegrationTest.class);

  private MultiProcessCluster mCluster;

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  @Test
  public void currentCompatibility() throws Exception {
    // Tests that the operation checks pass before and after restart.
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKWARDS_COMPATIBILITY)
        .setClusterName("BackwardsCompatibility")
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    mCluster.start();
    mCluster.waitForAllNodesRegistered(30 * Constants.SECOND_MS);
    Clients clients = mCluster.getClients();

    for (TestOp op : BackwardsCompatibilityJournalGenerator.OPS) {
      op.apply(clients);
      op.check(clients);
    }
    mCluster.stopMasters();
    mCluster.startMasters();
    for (TestOp op : BackwardsCompatibilityJournalGenerator.OPS) {
      op.check(clients);
    }
  }

  @Test
  public void readOldJournals() throws Exception {
    Assume.assumeTrue("Journals must be replayed by the same user that generated them, so this "
        + "test must be run as root", LoginUser.get().getName().equals("root"));
    // Starts a cluster from each old journal, and checks that all operation checks pass.
    List<Journal> journals = Arrays
        .asList(new File(BackwardsCompatibilityJournalGenerator.OLD_JOURNALS_RESOURCE).listFiles())
        .stream().map(f -> Journal.parse(f.getAbsolutePath()))
        .filter(Optional::isPresent)
        .map(o -> o.get())
        .collect(Collectors.toList());
    for (Journal journal : journals) {
      System.out.printf("Checking journal %s\n", journal.getDir());
      LOG.info("Checking journal %s\n", journal.getDir());
      Builder builder = MultiProcessCluster.newBuilder(PortCoordination.BACKWARDS_COMPATIBILITY)
          .setClusterName("BackwardsCompatibility")
          .setNumMasters(1)
          .setNumWorkers(1);
      if (journal.isBackup()) {
        builder.addProperty(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, journal.getDir());
      } else {
        File journalDir =
            AlluxioTestDirectory.createTemporaryDirectory("backwardsCompatibility-journal");
        FileUtils.copyDirectory(new File(journal.getDir()), new File(journalDir.getAbsolutePath()));
        builder.setNoFormat(true);
        builder.addProperty(PropertyKey.MASTER_JOURNAL_FOLDER, journalDir.getAbsolutePath());
      }
      mCluster = builder.build();
      try {
        mCluster.start();
        mCluster.waitForAllNodesRegistered(30 * Constants.SECOND_MS);
        Clients clients = mCluster.getClients();
        for (TestOp op : BackwardsCompatibilityJournalGenerator.OPS) {
          if (op.supportsVersion(journal.getVersion())) {
            op.check(clients);
          }
        }
      } finally {
        mCluster.destroy();
      }
    }
  }
}
