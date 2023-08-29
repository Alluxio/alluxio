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

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCachingFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FuseFileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Utils for ioctl.
 */
public class AlluxioFuseIoctlUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseIoctlUtils.class);

  /**
   * The ioctl commands type.
   */
  public enum IoctlCommands {
    CLEAR_METADATA(0),
    METADATA_SIZE(1);

    private final int mCmd;

    IoctlCommands(int cmd) {
      mCmd = cmd;
    }

    /**
     * @return the value of the IOC_COMMANDS
     */
    public int getValue() {
      return mCmd;
    }

    /**
     * @param val the value
     * @return the created instance
     */
    public static IoctlCommands fromValue(int val) {
      for (IoctlCommands cmd : IoctlCommands.values()) {
        if (val == cmd.getValue()) {
          return cmd;
        }
      }
      throw new IllegalArgumentException("No command " + val + " found");
    }
  }

  /**
   * Run the specific command.
   * @param fs the fs
   * @param uri the uri
   * @param buf the buf
   * @param fi the file info
   * @param conf the configuration
   * @param cmd the cmd
   * @return 0 if command success, otherwise -1
   */
  public static int runCommand(FileSystem fs, AlluxioURI uri, ByteBuffer buf,
      FuseFileInfo fi, AlluxioConfiguration conf, IoctlCommands cmd) {
    MetadataCachingFileSystem metadataCachingFileSystem = null;
    if (fs instanceof MetadataCachingFileSystem) {
      metadataCachingFileSystem = (MetadataCachingFileSystem) fs;
    }
    switch (cmd) {
      case CLEAR_METADATA:
        if (metadataCachingFileSystem == null) {
          LOG.error(String.format("Clear metadata command is " + "not supported when %s is false",
              PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
          return -ErrorCodes.EOPNOTSUPP();
        }
        String all = StandardCharsets.UTF_8.decode(buf).toString();
        if (all.equals("all")) {
          metadataCachingFileSystem.dropMetadataCacheAll();
        } else {
          metadataCachingFileSystem.dropMetadataCache(uri);
        }
        break;
      case METADATA_SIZE:
        if (metadataCachingFileSystem == null) {
          LOG.error(String.format("Clear metadata command is " + "not supported when %s is false",
              PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
          return -ErrorCodes.EOPNOTSUPP();
        }
        long size = metadataCachingFileSystem.getMetadataCacheSize();
        buf.position(0).limit(Long.toString(size).length());
        buf.put(Long.toString(size).getBytes());
        break;
      default:
        return -ErrorCodes.EOPNOTSUPP();
    }
    return 0;
  }
}
