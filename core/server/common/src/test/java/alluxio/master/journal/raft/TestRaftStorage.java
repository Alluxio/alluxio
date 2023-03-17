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

package alluxio.master.journal.raft;

import net.bytebuddy.utility.RandomString;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.server.storage.RaftStorageMetadata;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

class TestRaftStorage extends TemporaryFolder implements RaftStorage {
  RaftStorageDirectory mStorageDir;
  RaftStorageMetadataFile mMetaFile;

  @Override
  protected void before() throws Throwable {
    super.before();
    initialize();
  }

  @Override
  public void initialize() throws IOException {
    mStorageDir = new RaftStorageDirectory() {
      @Override
      public File getRoot() {
        return TestRaftStorage.super.getRoot();
      }

      @Override
      public boolean isHealthy() {
        return true;
      }
    };
    newFolder(relativizeToRoot(mStorageDir.getStateMachineDir().toPath()).toString());
    mMetaFile = new RaftStorageMetadataFile() {
      @Override
      public RaftStorageMetadata getMetadata() {
        return RaftStorageMetadata.getDefault();
      }

      @Override
      public void persist(RaftStorageMetadata newMetadata) {
      }
    };
  }

  @Override
  public RaftStorageDirectory getStorageDir() {
    return mStorageDir;
  }

  @Override
  public RaftStorageMetadataFile getMetadataFile() {
    return mMetaFile;
  }

  @Override
  public RaftServerConfigKeys.Log.CorruptionPolicy getLogCorruptionPolicy() {
    return RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION;
  }

  @Override
  public void close() throws IOException {
  }

  public void createSnapshotFolder(long term, long index) throws IOException {
    String snapshotFileName = SimpleStateMachineStorage.getSnapshotFileName(term, index);
    Path smDir = relativizeToRoot(mStorageDir.getStateMachineDir().toPath());
    File dir = newFolder(smDir.toString(), snapshotFileName);
    for (int i = 0; i < 10; i++) {
      String s = "dummy-file-" + i;
      File file = new File(dir, s);
      try (FileOutputStream outputStream = new FileOutputStream(file)) {
        outputStream.write(RandomString.make().getBytes());
      }
      MD5Hash md5Hash = MD5FileUtil.computeMd5ForFile(file);
      MD5FileUtil.saveMD5File(file, md5Hash);
    }
  }

  private Path relativizeToRoot(Path path) {
    return getRoot().toPath().relativize(path);
  }
}
