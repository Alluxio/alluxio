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

package alluxio.util.compression;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DirectoryMarshallerTest {
  @Parameterized.Parameters
  public static Collection<DirectoryMarshaller> data() {
    return Arrays.asList(new NoCompressionMarshaller(),
        new GzipMarshaller(),
        new TarGzMarshaller());
  }

  @Parameterized.Parameter
  public DirectoryMarshaller mMarshaller;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void emptyDir() throws Exception {
    Path empty = mFolder.newFolder("emptyDir").toPath();

    tarUntarTest(empty);
  }

  @Test
  public void oneFileDir() throws Exception {
    Path dir = mFolder.newFolder("oneFileDir").toPath();
    Path file = dir.resolve("file");
    Files.write(file, "test content".getBytes());

    tarUntarTest(dir);
  }

  @Test
  public void tenFileDir() throws Exception {
    Path dir = mFolder.newFolder("tenFileDir").toPath();
    for (int i = 0; i < 10; i++) {
      Path file = dir.resolve("file" + i);
      Files.write(file, ("test content" + i).getBytes());
    }

    tarUntarTest(dir);
  }

  @Test
  public void emptySubDir() throws Exception {
    Path dir = mFolder.newFolder("emptySubDir").toPath();
    Path subDir = dir.resolve("subDir");
    Files.createDirectory(subDir);

    tarUntarTest(dir);
  }

  @Test
  public void nested() throws Exception {
    Path dir = mFolder.newFolder("emptySubDir").toPath();
    Path current = dir;
    for (int i = 0; i < 10; i++) {
      Path newDir = current.resolve("dir" + i);
      Files.createDirectory(newDir);
      current = newDir;
    }
    Path file = current.resolve("file");
    Files.write(file, "hello world".getBytes());

    tarUntarTest(dir);
  }

  private void tarUntarTest(Path path) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    mMarshaller.write(path, baos);
    Path reconstructed = mFolder.newFolder("untarred").toPath();
    reconstructed.toFile().delete();
    mMarshaller.read(reconstructed, new ByteArrayInputStream(baos.toByteArray()));
    FileUtil.assertDirectoriesEqual(path, reconstructed);
  }
}
