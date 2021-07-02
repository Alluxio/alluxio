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

package alluxio.master.journalv0.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.journalv0.JournalWriter;
import alluxio.master.journalv0.MutableJournal;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.URIUtils;
import alluxio.util.UnderFileSystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link MutableJournal} based on UFS.
 */
@ThreadSafe
public class UfsMutableJournal extends UfsJournal implements MutableJournal {
  private static final Logger LOG = LoggerFactory.getLogger(UfsMutableJournal.class);

  /**
   * @param location the location for the journal
   */
  public UfsMutableJournal(URI location) {
    super(location);
  }

  @Override
  public void format() throws IOException {
    LOG.info("Formatting {}", mLocation);
    try (UnderFileSystem ufs = UnderFileSystem.Factory.create(mLocation.toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()))) {
      if (ufs.isDirectory(mLocation.toString())) {
        for (UfsStatus p : ufs.listStatus(mLocation.toString())) {
          URI childPath;
          try {
            childPath = URIUtils.appendPath(mLocation, p.getName());
          } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage());
          }
          boolean failedToDelete;
          if (p.isDirectory()) {
            failedToDelete = !ufs.deleteDirectory(childPath.toString(),
                DeleteOptions.defaults().setRecursive(true));
          } else {
            failedToDelete = !ufs.deleteFile(childPath.toString());
          }
          if (failedToDelete) {
            throw new IOException(String.format("Failed to delete %s", childPath));
          }
        }
      } else if (!ufs.mkdirs(mLocation.toString())) {
        throw new IOException(String.format("Failed to create %s", mLocation));
      }

      // Create a breadcrumb that indicates that the journal folder has been formatted.
      try {
        UnderFileSystemUtils.touch(ufs, URIUtils.appendPath(mLocation,
            ServerConfiguration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX)
                + System.currentTimeMillis())
            .toString());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  @Override
  public JournalWriter getWriter() {
    return new UfsJournalWriter(this);
  }
}
