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

package alluxio.master.backwards.compatibility;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.security.LoginUser;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;

/**
 * Tests that the current version can read journals from previous versions.
 *
 * This class has a main() method which can be used to populate src/test/resources/old_journals
 * with a journal and journal backup built from the current version. Whenever we release, we
 * generate new journal artifacts for the released version. The readOldJournals test will iterate
 * over all journal artifacts, replay them using the current version, and verify the results.
 *
 * To cover many types of journal entries, the test runs a series of TestOps, each of which makes
 * some independent modification to master state, and has a method to verify that the change was
 * correctly applied. To add a new TestOp, implement the TestOp interface or extend FsTestOp, and
 * add your TestOp to the OPS list. Also, either re-generate all journal artifacts from previous
 * versions (not easy at the moment, could add tooling for this later), or implements
 * supportsVersion to only match the latest version and future versions.
 */
public final class BackwardsCompatibilityJournalGenerator {
  // Path relative to tests/src/test
  public static final String OLD_JOURNALS_RESOURCE = "src/test/resources/old_journals";
  // Path is relative to alluxio home directory
  private static final String OLD_JOURNALS = PathUtils.concatPath("tests", OLD_JOURNALS_RESOURCE);

  public static final List<TestOp> OPS = ImmutableList.<TestOp>builder()
      .add(new CreateDirectory(),
          new CreateFile(),
          new Mount(),
          new AsyncPersist(),
          new DeleteFile(),
          new PersistFile(),
          new PersistDirectory(),
          new RenameFile(),
          new SetAcl()
      ).build();

  /**
   * Generates journal files to be used by the backwards compatibility test. The files are named
   * based on the current version defined in ProjectConstants.VERSION. Run this with each release,
   * and commit the created journal and snapshot into the git repository.
   *
   * @param args no args expected
   */
  public static void main(String[] args) throws Exception {
    if (!LoginUser.get().getName().equals("root")) {
      System.err.printf("Journals must be generated as root so that they can be replayed by root\n");
      System.exit(-1);
    }
    File journalDst = new File(OLD_JOURNALS,
        String.format("journal-%s", ProjectConstants.VERSION));
    if (journalDst.exists()) {
      System.err.printf("%s already exists, delete it first\n", journalDst.getAbsolutePath());
      System.exit(-1);
    }
    File backupDst = new File(OLD_JOURNALS,
        String.format("backup-%s", ProjectConstants.VERSION));
    if (backupDst.exists()) {
      System.err.printf("%s already exists, delete it first\n", backupDst.getAbsolutePath());
      System.exit(-1);
    }
    MultiProcessCluster cluster =
        MultiProcessCluster.newBuilder(PortCoordination.BACKWARDS_COMPATIBILITY)
            .setClusterName("BackwardsCompatibility")
            .setNumMasters(1)
            .setNumWorkers(1)
            .build();
    try {
      cluster.start();
      cluster.notifySuccess();
      cluster.waitForAllNodesRegistered(10 * Constants.SECOND_MS);
      for (TestOp op : OPS) {
        op.apply(Utils.getClients(cluster));
      }
      AlluxioURI backup = cluster.getMetaMasterClient()
          .backup(new File(OLD_JOURNALS).getAbsolutePath(), true)
          .getBackupUri();
      FileUtils.moveFile(new File(backup.getPath()), backupDst);
      cluster.stopMasters();
      FileUtils.copyDirectory(new File(cluster.getJournalDir()), journalDst);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      cluster.destroy();
    }
    System.out.printf("Artifacts successfully generated in %s and %s\n",
        journalDst.getAbsolutePath(), backupDst.getAbsolutePath());
  }
}
