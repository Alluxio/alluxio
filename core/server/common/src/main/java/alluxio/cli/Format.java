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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.NoopMaster;
import alluxio.master.ServiceUtils;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Formats the Alluxio file system.
 */
@ThreadSafe
public final class Format {
  private static final Logger LOG = LoggerFactory.getLogger(Format.class);
  private static final String USAGE = String.format("java -cp %s %s <MASTER/WORKER>",
      RuntimeConstants.ALLUXIO_JAR, Format.class.getCanonicalName());

  /**
   * The format mode.
   */
  public enum Mode {
    MASTER,
    WORKER,
  }

  /**
   * Formats the worker data folder.
   *
   * @param folder folder path
   */
  private static void formatWorkerDataFolder(String folder) throws IOException {
    Path path = Paths.get(folder);
    if (Files.exists(path)) {
      FileUtils.deletePathRecursively(folder);
    }
    Files.createDirectory(path);
    // For short-circuit read/write to work, others needs to be able to access this directory.
    // Therefore, default is 777 but if the user specifies the permissions, respect those instead.
    String permissions = Configuration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permissions);
    Files.setPosixFilePermissions(path, perms);
    FileUtils.setLocalDirStickyBit(path.toAbsolutePath().toString());
  }

  /**
   * Formats the Alluxio file system.
   *
   * @param args either {@code MASTER} or {@code WORKER}
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      LOG.info(USAGE);
      System.exit(-1);
    }
    // Set the process type as "MASTER" since format needs to access the journal like the master.
    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.MASTER);
    Mode mode = null;
    try {
      mode = Mode.valueOf(args[0].toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.error("Unrecognized format mode: {}", args[0]);
      LOG.error("Usage: {}", USAGE);
      System.exit(-1);
    }
    try {
      format(mode);
    } catch (Exception e) {
      LOG.error("Failed to format", e);
      System.exit(-1);
    }
    LOG.info("Formatting complete");
    System.exit(0);
  }

  /**
   * Formats the Alluxio file system.
   *
   * @param mode either {@code MASTER} or {@code WORKER}
   */
  public static void format(Mode mode) throws IOException {
    switch (mode) {
      case MASTER:
        URI journalLocation = JournalUtils.getJournalLocation();
        LOG.info("Formatting master journal: {}", journalLocation);
        JournalSystem journalSystem =
            new JournalSystem.Builder().setLocation(journalLocation).build();
        for (String masterServiceName : ServiceUtils.getMasterServiceNames()) {
          journalSystem.createJournal(new NoopMaster(masterServiceName));
        }
        journalSystem.format();
        break;
      case WORKER:
        String workerDataFolder = Configuration.get(PropertyKey.WORKER_DATA_FOLDER);
        LOG.info("Formatting worker data folder: {}", workerDataFolder);
        int storageLevels = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
        for (int level = 0; level < storageLevels; level++) {
          PropertyKey tierLevelDirPath =
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
          String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
          String name = "Data path for tier " + level;
          for (String dirPath : dirPaths) {
            String dirWorkerDataFolder = CommonUtils.getWorkerDataDirectory(dirPath);
            LOG.info("Formatting {}:{}", name, dirWorkerDataFolder);
            formatWorkerDataFolder(dirWorkerDataFolder);
          }
        }
        break;
      default:
        throw new RuntimeException(String.format("Unrecognized format mode: %s", mode));
    }
  }

  private Format() {} // prevent instantiation
}
