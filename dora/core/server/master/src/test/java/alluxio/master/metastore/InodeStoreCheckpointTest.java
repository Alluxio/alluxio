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

package alluxio.master.metastore;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterUtils;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.journal.checkpoint.CheckpointInputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class InodeStoreCheckpointTest {
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {MetastoreType.HEAP, 0},
        {MetastoreType.ROCKS, PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getDefaultValue()},
        {MetastoreType.ROCKS, 0}
    });
  }

  @Parameterized.Parameter(0)
  public MetastoreType mType;

  @Parameterized.Parameter(1)
  public int mCacheSize;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private InodeStore mBaseInodeStore;
  private InodeStore mNewInodeStore;

  private final MutableInodeDirectory mRoot =
      MutableInodeDirectory.create(0, -1, "", CreateDirectoryContext.defaults());

  private InodeStore createInodeStore() throws IOException {
    return MasterUtils.getInodeStoreFactory(mFolder.newFolder().getAbsolutePath())
        .apply(new InodeLockManager());
  }

  @Before
  public void before() throws IOException {
    Configuration.set(PropertyKey.MASTER_INODE_METASTORE, mType);
    Configuration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, mCacheSize);
    CreateDirectoryContext c = CreateDirectoryContext.defaults();
    CreateFileContext cf = CreateFileContext.defaults();
    mBaseInodeStore = createInodeStore();
    mBaseInodeStore.writeNewInode(MutableInodeDirectory.create(0, -1, "", c));
    mBaseInodeStore.writeNewInode(MutableInodeDirectory.create(1, 0, "one", c));
    mBaseInodeStore.writeNewInode(MutableInodeDirectory.create(2, 0, "two", c));
    mBaseInodeStore.writeNewInode(MutableInodeDirectory.create(3, 0, "three", c));
    mBaseInodeStore.remove(2L);
  }

  @After
  public void after() {
    Optional<Inode> root = mNewInodeStore.get(mRoot.getId());
    Assert.assertTrue(root.isPresent());
    Optional<Inode> one = mNewInodeStore.get(1);
    Assert.assertTrue(one.isPresent());
    Assert.assertEquals(0, one.get().getParentId());
    Assert.assertTrue(one.get().isDirectory());
    Assert.assertEquals("one", one.get().getName());
    Optional<Inode> two = mNewInodeStore.get(2);
    Assert.assertFalse(two.isPresent());
    Optional<Inode> three = mNewInodeStore.get(3);
    Assert.assertTrue(three.isPresent());
    Assert.assertEquals(0, three.get().getParentId());
    Assert.assertEquals("three", three.get().getName());

    mBaseInodeStore.close();
    mNewInodeStore.close();
  }

  @Test
  public void testOutputStream() throws IOException, InterruptedException {
    File checkpoint = mFolder.newFile("checkpoint");
    try (OutputStream outputStream = Files.newOutputStream(checkpoint.toPath())) {
      mBaseInodeStore.writeToCheckpoint(outputStream);
    }
    mNewInodeStore = createInodeStore();
    try (CheckpointInputStream inputStream =
             new CheckpointInputStream(Files.newInputStream(checkpoint.toPath()))) {
      mNewInodeStore.restoreFromCheckpoint(inputStream);
    }
  }

  @Test
  public void testDirectory() throws IOException {
    File dir = mFolder.newFolder("checkpoint");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    mBaseInodeStore.writeToCheckpoint(dir, executor).join();
    mNewInodeStore = createInodeStore();
    mNewInodeStore.restoreFromCheckpoint(dir, executor).join();
  }
}
