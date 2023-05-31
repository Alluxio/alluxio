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

package alluxio.master.journal.checkpoint;

import net.bytebuddy.utility.RandomString;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CheckpointStreamTest {
  @Parameterized.Parameters
  public static Collection<CheckpointType> data() {
    return Arrays.asList(CheckpointType.values());
  }

  @Parameterized.Parameter
  public CheckpointType mType;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void regularStreamTest() throws IOException {
    File file = mFolder.newFile();
    byte[] contents = RandomString.make().getBytes();
    try (CheckpointOutputStream outputStream =
             new CheckpointOutputStream(Files.newOutputStream(file.toPath()), mType)) {
      outputStream.write(contents);
    }
    byte[] retrieved = new byte[contents.length];
    try (CheckpointInputStream s = new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      Assert.assertEquals(mType, s.getType());
      s.read(retrieved);
    }
    Assert.assertArrayEquals(contents, retrieved);
  }

  @Test
  public void optimizedStreamTest() throws IOException {
    File file = mFolder.newFile();
    MessageDigest md5Out = MD5Hash.getDigester();
    byte[] contents = RandomString.make().getBytes();
    try (CheckpointOutputStream outputStream =
             new CheckpointOutputStream(new OptimizedCheckpointOutputStream(file, md5Out), mType)) {
      outputStream.write(contents);
    }
    MD5FileUtil.saveMD5File(file, new MD5Hash(md5Out.digest()));
    MessageDigest md5In = MD5Hash.getDigester();
    byte[] retrieved = new byte[contents.length];
    try (CheckpointInputStream s = new OptimizedCheckpointInputStream(file, md5In)) {
      Assert.assertEquals(mType, s.getType());
      s.read(retrieved);
    }
    MD5FileUtil.verifySavedMD5(file, new MD5Hash(md5In.digest()));
    Assert.assertArrayEquals(contents, retrieved);
  }
}
