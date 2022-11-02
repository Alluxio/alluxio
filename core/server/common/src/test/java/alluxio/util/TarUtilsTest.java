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

package alluxio.util;

import static org.mockito.ArgumentMatchers.any;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Units tests for {@link TarUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TarUtils.class)
public final class TarUtilsTest {
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

  @Test
  public void testLargePosixUserAndGroupIds() throws Exception {
    // when the TarArchiveEntry(File, String) constructor is called (as is used in
    // TarUtils#writeTarGz), return a new instance of TarArchiveEntry that has a group id greater
    // than the max id
    Long largeId = TarArchiveEntry.MAXID + 100;
    PowerMockito.whenNew(TarArchiveEntry.class)
        .withParameterTypes(File.class, String.class)
        .withArguments(any())
        .thenAnswer(i -> {
          TarArchiveEntry spy = PowerMockito.spy(
              TarArchiveEntry.class.getConstructor(File.class, String.class)
                  .newInstance(i.getArguments()));
          PowerMockito.when(spy.getLongGroupId()).thenReturn(largeId);
          PowerMockito.when(spy.getLongUserId()).thenReturn(largeId);
          return spy;
        });

    Path empty = mFolder.newFolder("emptyDir").toPath();
    tarUntarTest(empty);
  }

  private void tarUntarTest(Path path) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TarUtils.writeTarGz(path, baos);
    Path reconstructed = mFolder.newFolder("untarred").toPath();
    reconstructed.toFile().delete();
    TarUtils.readTarGz(reconstructed, new ByteArrayInputStream(baos.toByteArray()));
    FileUtil.assertDirectoriesEqual(path, reconstructed);
  }
}
