package tachyon.master;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertEquals;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tachyon.client.TachyonFS;

import java.io.IOException;

public class PinTest {

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void recursivePinness() throws Exception {
    int dir0Id = mTfs.getFileId("/");

    mTfs.mkdir("/myFolder");
    int dir1Id = mTfs.getFileId("/myFolder");

    int fileId = mTfs.createFile("/myFolder/myFile");
    assertFalse(mTfs.getFile(fileId).needPin());

    mTfs.pinFile(fileId);
    assertTrue(mTfs.getFile("/myFolder/myFile").needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    mTfs.unpinFile(fileId);
    assertFalse(mTfs.getFile("/myFolder/myFile").needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer> newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.pinFile(dir1Id);
    assertTrue(mTfs.getFile("/myFolder/myFile").needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    // Same with unpinning.
    mTfs.unpinFile(dir0Id);
    assertFalse(mTfs.getFile("/myFolder/myFile").needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer> newHashSet());

    // The last pin command always wins.
    mTfs.pinFile(fileId);
    assertTrue(mTfs.getFile("/myFolder/myFile").needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    int rootId = mTfs.getFileId("/");
    mTfs.pinFile(rootId);

    // Child file should be pinned
    int file0Id = mTfs.createFile("/file0");
    assertTrue(mMasterInfo.getClientFileInfo(file0Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // Child folder should be pinned
    mTfs.mkdir("/folder");
    int folderId = mTfs.getFileId("/folder");
    assertTrue(mMasterInfo.getClientFileInfo(folderId).isPinned);

    // Granchild file also pinned
    int file1Id = mTfs.createFile("/folder/file1");
    assertTrue(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id, file1Id));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.unpinFile(folderId);
    assertFalse(mMasterInfo.getClientFileInfo(folderId).isPinned);
    assertFalse(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // And new grandchildren should be unpinned too.
    int file2Id = mTfs.createFile("/folder/file2");
    assertFalse(mMasterInfo.getClientFileInfo(file2Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // But toplevel children still should be pinned!
    int file3Id = mTfs.createFile("/file3");
    assertTrue(mMasterInfo.getClientFileInfo(file3Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id, file3Id));
  }
}
