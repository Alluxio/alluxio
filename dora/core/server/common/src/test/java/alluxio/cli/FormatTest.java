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

package alluxio.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;

/**
 * Unit tests for {@link Format}.
 */
public final class FormatTest {

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Test
  public void formatWorker() throws Exception {
    final int storageLevels = 1;
    final String perms = "rwx------";
    String workerDataFolder;
    final File[] dirs = new File[] {
        mTemporaryFolder.newFolder("level0")
    };
    for (File dir : dirs) {
      workerDataFolder = CommonUtils.getWorkerDataDirectory(dir.getPath(),
          Configuration.global());
      FileUtils.createDir(PathUtils.concatPath(workerDataFolder, "subdir"));
      FileUtils.createFile(PathUtils.concatPath(workerDataFolder, "file"));
    }
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0), dirs[0].getPath());
        put(PropertyKey.WORKER_TIERED_STORE_LEVELS, storageLevels);
        put(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, perms);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      Format.format(Format.Mode.WORKER, Configuration.global());
    }
  }

  @Test
  public void formatWorkerDeleteFileSameName() throws Exception {
    final int storageLevels = 1;
    String workerDataFolder;
    final File[] dirs = new File[] {
        mTemporaryFolder.newFolder("level0")
    };
    // Have files of same name as the target worker data dir in each tier
    for (File dir : dirs) {
      workerDataFolder = CommonUtils.getWorkerDataDirectory(dir.getPath(),
          Configuration.global());
      FileUtils.createFile(workerDataFolder);
    }
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0), dirs[0].getPath());
        put(PropertyKey.WORKER_TIERED_STORE_LEVELS, storageLevels);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      final String perms = Configuration.getString(
          PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);
      Format.format(Format.Mode.WORKER, Configuration.global());
      for (File dir : dirs) {
        workerDataFolder = CommonUtils.getWorkerDataDirectory(dir.getPath(),
            Configuration.global());
        assertTrue(Files.isDirectory(Paths.get(workerDataFolder)));
        assertEquals(PosixFilePermissions.fromString(perms), Files.getPosixFilePermissions(Paths
            .get(workerDataFolder)));
        try (DirectoryStream<Path> directoryStream =
                 Files.newDirectoryStream(Paths.get(workerDataFolder))) {
          for (Path child : directoryStream) {
            fail("No sub dirs or files are expected in " + child.toString());
          }
        }
      }
    }
  }
}
