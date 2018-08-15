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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.master.MasterClientConfig;
import alluxio.master.file.meta.PersistenceState;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.Builder;
import alluxio.multi.process.PortCoordination;
import alluxio.security.authorization.AclEntry;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.wire.SetAclAction;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
public final class BackwardsCompatibilityIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BackwardsCompatibilityIntegrationTest.class);

  // Path relative to tests/src/test
  private static final String OLD_JOURNALS_RESOURCE = "src/test/resources/old_journals";
  // Path is relative to alluxio home directory
  private static final String OLD_JOURNALS = PathUtils.concatPath("tests", OLD_JOURNALS_RESOURCE);

  // Local filesystem directory to mount in the mount operation
  private static final String LOCAL_FS_MOUNT_DIR = "/tmp/alluxioMount";

  private static final List<TestOp> OPS = ImmutableList.<TestOp>builder()
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

  private MultiProcessCluster mCluster;

  @BeforeClass
  public static void beforeClass() {
    new File(LOCAL_FS_MOUNT_DIR).mkdirs();
  }

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  private interface TestOp {
    /**
     * Applies the test operation.
     *
     * @param clients Alluxio clients for performing the operation
     */
    void apply(Clients clients) throws Exception;

    /**
     * Verifies the result of the test operation.
     *
     * @param clients Alluxio clients for performing the verification
     */
    void check(Clients clients) throws Exception;

    /**
     * @param version a version, e.g. 1.8.0, 1.9.0, etc
     * @return whether this operation is supported in the given version
     */
    default boolean supportsVersion(Version version) {
      // Support all versions by default
      return true;
    }
  }

  public abstract static class FsTestOp implements TestOp {
    @Override
    public void apply(Clients clients) throws Exception {
      apply(clients.getFs());
    }

    protected abstract void apply(FileSystem fs) throws Exception;

    @Override
    public void check(Clients clients) throws Exception {
      check(clients.getFs());
    }

    protected abstract void check(FileSystem fs) throws Exception;
  }

  private static class CreateDirectory extends FsTestOp {
    private static final AlluxioURI DIR = new AlluxioURI("/createDirectory");

    @Override
    public void apply(FileSystem fs) throws Exception {
      fs.createDirectory(DIR);
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertTrue(fs.exists(DIR));
    }
  }

  private static class CreateFile extends FsTestOp {
    private static final AlluxioURI PATH = new AlluxioURI("/createFile");

    @Override
    public void apply(FileSystem fs) throws Exception {
      createFile(fs, PATH);
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertTrue("Created file should exist", fs.exists(PATH));
    }
  }

  private static class Mount extends FsTestOp {
    private static final AlluxioURI ALLUXIO_PATH = new AlluxioURI("/mount");

    // This creates a requirement that /tmp exists and the test is allowed to read /tmp
    private static final AlluxioURI UFS_PATH = new AlluxioURI(LOCAL_FS_MOUNT_DIR);

    @Override
    public void apply(FileSystem fs) throws Exception {
      fs.mount(ALLUXIO_PATH, UFS_PATH);
      fs.unmount(ALLUXIO_PATH);
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertFalse("Mounted and unmounted directory should not exist", fs.exists(ALLUXIO_PATH));
    }
  }

  private static class AsyncPersist extends FsTestOp {
    private static final AlluxioURI FILE = new AlluxioURI("/asyncPersist");

    @Override
    public void apply(FileSystem fs) throws Exception {
      try (FileOutStream out = fs.createFile(FILE, CreateFileOptions.defaults()
          .setBlockSizeBytes(Constants.KB).setWriteType(WriteType.ASYNC_THROUGH))) {
        out.write("test".getBytes());
      }
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      URIStatus status = fs.getStatus(FILE);
      assertThat("Async persisted file should be PERSISTED or TO_BE_PERSISTED",
          Arrays.asList(PersistenceState.PERSISTED.name(), PersistenceState.TO_BE_PERSISTED.name()),
          CoreMatchers.hasItem(status.getPersistenceState()));
    }
  }

  private static class DeleteFile extends FsTestOp {
    private static final AlluxioURI PATH = new AlluxioURI("/pathToDelete");

    @Override
    public void apply(FileSystem fs) throws Exception {
      createFile(fs, PATH);
      fs.delete(PATH);
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertFalse("Deleted file should not exist", fs.exists(PATH));
    }
  }

  private static class PersistFile implements TestOp {
    private static final AlluxioURI PATH = new AlluxioURI("/fileToPersist");

    @Override
    public void apply(Clients clients) throws Exception {
      FileSystem fs = clients.getFs();
      createFile(fs, PATH);
      clients.getFileSystemMaster().scheduleAsyncPersist(PATH);
      CommonUtils.waitFor("file to be async persisted", () -> {
        try {
          return fs.getStatus(PATH).isPersisted();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
    }

    @Override
    public void check(Clients clients) throws Exception {
      assertTrue("Persisted file should be persisted",
          clients.getFs().getStatus(PATH).isPersisted());
    }
  }

  private static class PersistDirectory extends FsTestOp {
    private static final AlluxioURI DIR = new AlluxioURI("/dirToPersist");
    private static final AlluxioURI INNER_FILE = new AlluxioURI("/dirToPersist/innerFile");

    @Override
    public void apply(FileSystem fs) throws Exception {
      fs.createDirectory(DIR);
      createFile(fs, INNER_FILE);
      fs.setAttribute(INNER_FILE, SetAttributeOptions.defaults().setPersisted(true));
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertTrue("Parent directory should be persisted", fs.getStatus(DIR).isPersisted());
      assertTrue("Persisted file should be persisted", fs.getStatus(INNER_FILE).isPersisted());
    }
  }

  private static class RenameFile extends FsTestOp {
    private static final AlluxioURI SRC = new AlluxioURI("/fileToRename");
    private static final AlluxioURI DST = new AlluxioURI("/fileRenameTarget");

    @Override
    public void apply(FileSystem fs) throws Exception {
      createFile(fs, SRC);
      fs.rename(SRC, DST);
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertFalse("Rename src should not exist", fs.exists(SRC));
      assertTrue("Rename dst should exist", fs.exists(DST));
    }
  }

  private static class SetAcl extends FsTestOp {
    private static final AlluxioURI DIR = new AlluxioURI("/dirToSetAcl");
    private static final String ACL_STRING = "group::rwx";

    @Override
    public void apply(FileSystem fs) throws Exception {
      fs.createDirectory(DIR);
      fs.setAcl(DIR, SetAclAction.MODIFY, Arrays.asList(AclEntry.fromCliString(ACL_STRING)));
    }

    @Override
    public void check(FileSystem fs) throws Exception {
      assertThat("Acl should be set", fs.getStatus(DIR).getAcl().toString(),
          containsString(ACL_STRING));
    }

    @Override
    public boolean supportsVersion(Version version) {
      return version.compareTo(new Version(1, 9, 0)) >= 0;
    }
  }

  @Test
  public void currentCompatibility() throws Exception {
    // Tests that the operation checks pass before and after restart.
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_FAILURE)
        .setClusterName("BackwardsCompatibility")
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    mCluster.start();
    mCluster.waitForAllNodesRegistered(10 * Constants.SECOND_MS);
    Clients clients = getClients(mCluster);

    for (TestOp op : OPS) {
      op.apply(clients);
      op.check(clients);
    }
    mCluster.stopMasters();
    mCluster.startMasters();
    for (TestOp op : OPS) {
      op.check(clients);
    }
  }

  @Test
  public void readOldJournals() throws Exception {
    // Starts a cluster from each old journal, and checks that all operation checks pass.
    List<Journal> journals = Arrays.asList(new File(OLD_JOURNALS_RESOURCE).listFiles()).stream()
        .map(f -> Journal.parse(f.getAbsolutePath()))
        .filter(Optional::isPresent)
        .map(o -> o.get())
        .collect(Collectors.toList());
    for (Journal journal : journals) {
      System.out.printf("Checking journal %s\n", journal.getDir());
      LOG.info("Checking journal %s\n", journal.getDir());
      Builder builder = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_FAILURE)
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
        mCluster.waitForAllNodesRegistered(10 * Constants.SECOND_MS);
        Clients clients = getClients(mCluster);
        for (TestOp op : OPS) {
          if (op.supportsVersion(journal.getVersion())) {
            op.check(clients);
          }
        }
      } finally {
        mCluster.destroy();
      }
    }
  }

  private static void createFile(FileSystem fs, AlluxioURI path) throws Exception {
    try (FileOutStream out =
        fs.createFile(path, CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB))) {
      out.write("test".getBytes());
    }
  }

  private static Clients getClients(MultiProcessCluster mCluster) {
    return new Clients(mCluster.getFileSystemClient(), FileSystemMasterClient.Factory.create(
        MasterClientConfig.defaults().withMasterInquireClient(mCluster.getMasterInquireClient())));
  }

  /**
   * Container for various Alluxio clients.
   */
  private static class Clients {
    private final FileSystem mFs;
    private final FileSystemMasterClient mFsMaster;

    public Clients(FileSystem fs, FileSystemMasterClient fsm) {
      mFs = fs;
      mFsMaster = fsm;
    }

    public FileSystem getFs() {
      return mFs;
    }

    public FileSystemMasterClient getFileSystemMaster() {
      return mFsMaster;
    }
  }

  private static class Version implements Comparable<Version> {
    private final int mMajor;
    private final int mMinor;
    private final int mPatch;

    private Version(int major, int minor, int patch) {
      mMajor = major;
      mMinor = minor;
      mPatch = patch;
    }

    @Override
    public int compareTo(Version o) {
      if (mMajor != o.mMajor) {
        return mMajor - o.mMajor;
      }
      if (mMinor != o.mMinor) {
        return mMinor - o.mMinor;
      }
      if (mPatch != o.mPatch) {
        return mPatch - o.mPatch;
      }
      return 0;
    }

    public String toString() {
      return String.format("%d.%d.%d", mMajor, mMinor, mPatch);
    }
  }

  private static class Journal {
    private static final String VERSION = "(?<major>\\d+).(?<minor>\\d+).(?<patch>\\d+).*";
    private static final Pattern JOURNAL_VERSION_RE = Pattern.compile("journal-" + VERSION);
    private static final Pattern BACKUP_VERSION_RE = Pattern.compile("backup-" + VERSION);

    private final boolean mIsBackup;
    private final String mDir;
    private final Version mVersion;

    private Journal(boolean isBackup, String dir, Version version) {
      mIsBackup = isBackup;
      mDir = dir;
      mVersion = version;
    }

    /**
     * @return whether this is a journal backup, as opposed to a full journal directory
     */
    public boolean isBackup() {
      return mIsBackup;
    }

    /**
     * @return the absolute directory of the journal
     */
    public String getDir() {
      return mDir;
    }

    /**
     * @return the version of the journal
     */
    public Version getVersion() {
      return mVersion;
    }

    /**
     * @param path an absolute path of an Alluxio journal or journal backup
     * @return the constructed Journal
     */
    public static Optional<Journal> parse(String path) {
      File f = new File(path);
      boolean isBackup = false;
      Matcher matcher = JOURNAL_VERSION_RE.matcher(f.getName());
      if (!matcher.matches()) {
        isBackup = true;
        matcher = BACKUP_VERSION_RE.matcher(f.getName());
        if (!matcher.matches()) {
          return Optional.empty();
        }
      }
      int major = Integer.parseInt(matcher.group("major"));
      int minor = Integer.parseInt(matcher.group("minor"));
      int patch = Integer.parseInt(matcher.group("patch"));
      return Optional.of(new Journal(isBackup, path, new Version(major, minor, patch)));
    }
  }

  /**
   * Generates journal files to be used by the backwards compatibility test. The files are named
   * based on the current version defined in ProjectConstants.VERSION. Run this with each release,
   * and commit the created journal and snapshot into the git repository.
   *
   * @param args no args expected
   */
  public static void main(String[] args) throws Exception {
    File journalDst = new File(OLD_JOURNALS,
        String.format("journal-%s", ProjectConstants.VERSION));
    if (journalDst.exists()) {
      System.err.printf("%s already exists, delete it first", journalDst.getAbsolutePath());
      System.exit(-1);
    }
    File backupDst = new File(OLD_JOURNALS,
        String.format("backup-%s", ProjectConstants.VERSION));
    if (backupDst.exists()) {
      System.err.printf("%s already exists, delete it first", backupDst.getAbsolutePath());
      System.exit(-1);
    }
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_FAILURE)
        .setClusterName("BackwardsCompatibility")
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    try {
      cluster.start();
      cluster.waitForAllNodesRegistered(10 * Constants.SECOND_MS);
      for (TestOp op : OPS) {
        op.apply(getClients(cluster));
      }
      AlluxioURI backup = cluster.getMetaMasterClient()
          .backup(new File(OLD_JOURNALS).getAbsolutePath(), true)
          .getBackupUri();
      FileUtils.moveFile(new File(backup.getPath()), backupDst);
      cluster.stopMasters();
      FileUtils.copyDirectory(new File(cluster.getJournalDir()), journalDst);
    } finally {
      cluster.destroy();
    }
  }
}
