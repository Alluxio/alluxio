package tachyon.master;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

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
    int dir0Id = mTfs.getFileId(new TachyonURI("/"));
    TachyonURI folder = new TachyonURI("/myFolder");
    TachyonURI file = new TachyonURI("/myFolder/myFile");

    mTfs.mkdir(folder);
    int dir1Id = mTfs.getFileId(folder);

    int fileId = mTfs.createFile(file);
    assertFalse(mTfs.getFile(fileId).needPin());

    mTfs.pinFile(fileId);
    assertTrue(mTfs.getFile(file).needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    mTfs.unpinFile(fileId);
    assertFalse(mTfs.getFile(file).needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.pinFile(dir1Id);
    assertTrue(mTfs.getFile(file).needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    // Same with unpinning.
    mTfs.unpinFile(dir0Id);
    assertFalse(mTfs.getFile(file).needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer>newHashSet());

    // The last pin command always wins.
    mTfs.pinFile(fileId);
    assertTrue(mTfs.getFile(file).needPin());
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    int rootId = mTfs.getFileId(new TachyonURI("/"));
    mTfs.pinFile(rootId);

    // Child file should be pinned
    int file0Id = mTfs.createFile(new TachyonURI("/file0"));
    assertTrue(mMasterInfo.getClientFileInfo(file0Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // Child folder should be pinned
    mTfs.mkdir(new TachyonURI("/folder"));
    int folderId = mTfs.getFileId(new TachyonURI("/folder"));
    assertTrue(mMasterInfo.getClientFileInfo(folderId).isPinned);

    // Granchild file also pinned
    int file1Id = mTfs.createFile(new TachyonURI("/folder/file1"));
    assertTrue(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id, file1Id));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.unpinFile(folderId);
    assertFalse(mMasterInfo.getClientFileInfo(folderId).isPinned);
    assertFalse(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // And new grandchildren should be unpinned too.
    int file2Id = mTfs.createFile(new TachyonURI("/folder/file2"));
    assertFalse(mMasterInfo.getClientFileInfo(file2Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // But toplevel children still should be pinned!
    int file3Id = mTfs.createFile(new TachyonURI("/file3"));
    assertTrue(mMasterInfo.getClientFileInfo(file3Id).isPinned);
    assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id, file3Id));
  }
}
