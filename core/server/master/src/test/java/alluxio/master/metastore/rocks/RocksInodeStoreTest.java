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

package alluxio.master.metastore.rocks;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.metastore.InodeStore.WriteBatch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class RocksInodeStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void batchWrite() throws IOException {
    RocksInodeStore store = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    WriteBatch batch = store.createWriteBatch();
    for (int i = 1; i < 20; i++) {
      batch.writeInode(
          MutableInodeDirectory.create(i, 0, "dir" + i, CreateDirectoryContext.defaults()));
    }
    batch.commit();
    for (int i = 1; i < 20; i++) {
      assertEquals("dir" + i, store.get(i).get().getName());
    }
  }

  @Test
  public void toStringEntries() throws IOException {
    RocksInodeStore store = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    assertEquals("", store.toStringEntries());

    store.writeInode(MutableInodeDirectory.create(1, 0, "dir", CreateDirectoryContext.defaults()));
    assertEquals("dir", store.get(1).get().getName());
    assertThat(store.toStringEntries(), containsString("name=dir"));
  }
}
