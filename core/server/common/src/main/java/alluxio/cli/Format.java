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
import alluxio.PropertyKeyFormat;
import alluxio.RuntimeConstants;
import alluxio.ServerUtils;
import alluxio.master.journal.MutableJournal;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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

  private static void formatFolder(String name, String folder) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(folder);
    LOG.info("Formatting {}:{}", name, folder);
    if (ufs.isDirectory(folder)) {
      for (UnderFileStatus p : ufs.listStatus(folder)) {
        String childPath = PathUtils.concatPath(folder, p.getName());
        boolean failedToDelete;
        if (p.isDirectory()) {
          failedToDelete = !ufs.deleteDirectory(childPath,
              DeleteOptions.defaults().setRecursive(true));
        } else {
          failedToDelete = !ufs.deleteFile(childPath);
        }
        if (failedToDelete) {
          throw new IOException(String.format("Failed to delete %s", childPath));
        }
      }
    } else if (!ufs.mkdirs(folder)) {
      throw new IOException(String.format("Failed to create dir %s", folder));
    }
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
    try {
      format(Mode.valueOf(args[0].toUpperCase()));
    } catch (IllegalArgumentException e) {
      LOG.error("Unrecognized format mode: {}", args[0]);
      LOG.error("Usage: {}", USAGE);
      System.exit(-1);
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
   * @throws IOException if a non-Alluxio related exception occurs
   */
  public static void format(Mode mode) throws IOException {
    switch (mode) {
      case MASTER:
        String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
        LOG.info("MASTER JOURNAL: {}", masterJournal);
        MutableJournal.Factory factory;
        try {
          factory = new MutableJournal.Factory(new URI(masterJournal));
        } catch (URISyntaxException e) {
          throw new IOException(e.getMessage());
        }
        for (String masterServiceName : ServerUtils.getMasterServiceNames()) {
          factory.create(masterServiceName).format();
        }
        break;
      case WORKER:
        String workerDataFolder = Configuration.get(PropertyKey.WORKER_DATA_FOLDER);
        int storageLevels = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
        for (int level = 0; level < storageLevels; level++) {
          PropertyKey tierLevelDirPath =
              PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(level);
          String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
          String name = "TIER_" + level + "_DIR_PATH";
          for (String dirPath : dirPaths) {
            String dirWorkerDataFolder = PathUtils.concatPath(dirPath.trim(), workerDataFolder);
            UnderFileSystem ufs = UnderFileSystem.Factory.get(dirWorkerDataFolder);
            if (ufs.isDirectory(dirWorkerDataFolder)) {
              try {
                formatFolder(name, dirWorkerDataFolder);
              } catch (IOException e) {
                throw new RuntimeException(String
                    .format("Failed to format worker data folder %s due to %s", dirWorkerDataFolder,
                        e.getMessage()));
              }
            }
          }
        }
        break;
      default:
        throw new RuntimeException(String.format("Unrecognized format mode: %s", mode));
    }
  }

  private Format() {} // prevent instantiation
}
