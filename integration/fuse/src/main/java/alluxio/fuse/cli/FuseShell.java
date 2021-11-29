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

package alluxio.fuse.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.fuse.cli.command.MetadataCacheCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Fuse shell to run Fuse special commands.
 */
public final class FuseShell {
  private static final Logger LOG = LoggerFactory.getLogger(FuseShell.class);
  public static final String ALLUXIO_CLI_PATH = "/.alluxiocli";

  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;

  /**
   * Creates a new instance of {@link FuseShell}.
   * @param fs Alluxio file system
   * @param conf Alluxio configuration
   */
  public FuseShell(FileSystem fs, AlluxioConfiguration conf) {
    mFileSystem = fs;
    mConf = conf;
  }

  /**
   * Checks if the given uri contains Fuse special command.
   *
   * @param uri check whether the uri contains Fuse special command
   * @return true of if the path is a special path, otherwise false
   */
  public boolean isFuseSpecialCommand(AlluxioURI uri) {
    int index = uri.getPath().lastIndexOf(AlluxioURI.SEPARATOR);
    return index != -1 && uri.getPath().substring(index).startsWith(ALLUXIO_CLI_PATH);
  }

  /**
   * @param uri that include command information
   * @return a mock URIStatus instance
   */
  public URIStatus runCommand(AlluxioURI uri) throws InvalidArgumentException {
    // TODO(bingzheng): extend some other operations.
    AlluxioURI path = uri.getParent();
    int index = uri.getPath().lastIndexOf(ALLUXIO_CLI_PATH);
    String cmdsInfo = uri.getPath().substring(index);
    String [] cmds = cmdsInfo.split("\\.");

    if (cmds.length <= 2) {
      logUsage();
      throw new InvalidArgumentException("Command is needed in Fuse shell");
    }

    String cmdType = cmds[2];
    switch (CommandType.fromValue(cmdType)) {
      case METADATA_CACHE:
        MetadataCacheCommand command = new MetadataCacheCommand(mFileSystem, mConf);
        return command.run(path, Arrays.copyOfRange(cmds, 3, cmds.length));
      default:
        throw new InvalidArgumentException("Fuse shell command not found");
    }
  }

  private void logUsage() {
    // TODO(lu) better log fuse shell description
    LOG.info(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED.getDescription());
  }

  /**
   * Fuse command type.
   */
  public enum CommandType {
    /**
     * Command will operate the metadata cache.
     */
    METADATA_CACHE("metadatacache"),
    ;

    final String mValue;

    @Override
    public String toString() {
      return mValue;
    }

    CommandType(String command) {
      mValue = command;
    }

    /**
     * @param value  that need to be transformed to CommandType
     * @return CommandType related to the value
     */
    public static CommandType fromValue(final String value) {
      for (CommandType ct : CommandType.values()) {
        if (ct.mValue.equals(value)) {
          return ct;
        }
      }
      return METADATA_CACHE;
    }
  }
}
