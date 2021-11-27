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
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fuse client tool to clear metadata cache and other operation.
 */
public final class AlluxioFuseClientTools {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseClientTools.class);
  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;
  public static final String ALLUXIO_RESERVED_DIR = "/.alluxiocli";

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

  /**
   * Creates a new instance of {@link AlluxioFuseClientTools}.
   * @param fs Alluxio file system
   * @param conf Alluxio configuration
   */
  public AlluxioFuseClientTools(FileSystem fs, AlluxioConfiguration conf) {
    mFileSystem = fs;
    mConf = conf;
  }

  /**
   * @param uri check whether the uri is a reserved path
   * @return true of if the path is a special path, otherwise false
   */
  public boolean isReservedPath(AlluxioURI uri) {
    int index = uri.getPath().lastIndexOf("/");
    if (index != -1 && uri.getPath().substring(index).startsWith(ALLUXIO_RESERVED_DIR)) {
      return true;
    }
    return false;
  }

  /**
   * User will use "ls -l /path/.alluxiocli.[Command].[Subcommand].[args]" to
   * do special operation, the /path/ will be the fuse path that specific a
   * directory.
   * @param uri that need to be parsed
   * @return a ClientCommand instance
   */
  private ClientCommand parseCommand(AlluxioURI uri) {
    AlluxioURI path = uri.getParent();
    int index = uri.getPath().lastIndexOf("/");
    String cmdsInfo = uri.getPath().substring(index);
    String [] cmds = cmdsInfo.split("\\.");
    if (cmds.length < 4 || cmds.length > 5) {
      LOG.error("Wrong number operation parameters {}", cmdsInfo);
      return null;
    }
    return new ClientCommand(path, cmds[2], cmds[3], cmds.length == 5 ? cmds[4] : null);
  }

  private URIStatus metadataCacheCommand(AlluxioURI path, String cmd) {
    URIStatus status = null;
    if (!mConf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED)) {
      return null;
    }
    switch (cmd) {
      case "dropAll":
        ((MetadataCachingBaseFileSystem) mFileSystem).dropMetadataCacheAll();
        status = new URIStatus(new FileInfo().setCompleted(true));
        break;
      case "drop":
        ((MetadataCachingBaseFileSystem) mFileSystem).dropMetadataCache(path);
        status = new URIStatus(new FileInfo().setCompleted(true));
        break;
      case "size":
        // The 'ls -al' command will show metadata cache size in the <filesize> field.
        long size = ((MetadataCachingBaseFileSystem) mFileSystem).getMetadataCacheSize();
        status = new URIStatus(new FileInfo().setLength(size).setCompleted(true));
        break;
      default:
        LOG.error("Unsupported metadata cache command {}", cmd);
        break;
    }
    return status;
  }

  /**
   * @param uri that include command information
   * @return a mock URIStatus instance
   */
  public URIStatus runCommand(AlluxioURI uri) {
    // TODO(bingzheng): extend some other operation.
    URIStatus status = null;
    ClientCommand command = parseCommand(uri);
    if (command == null) {
      LOG.error("Failed to parse client command {}", uri.getPath());
      return null;
    }
    switch (CommandType.fromValue(command.getCommandType())) {
      case METADATA_CACHE:
        status = metadataCacheCommand(command.getPath(), command.getCommand());
        break;
      default:
        LOG.error("Unsupported client command {}", uri.getPath());
        break;
    }
    return status;
  }

  /**
   * Special command on the fuse client side.
   */
  public class ClientCommand {
    private final AlluxioURI mPath;
    private final String mCommandType;
    private final String mCmd;
    private final String mArgs;

  /**
   * Creates a new instance of {@link ClientCommand}.
   * @param path derived from AlluxioUri
   * @param commandType base command type, for now only 'metadatacache' is supported
   * @param cmd subcommand specify the real action
   * @param args bonds to cmd, not use for now
   */
    public ClientCommand(AlluxioURI path, String commandType, String cmd, String args) {
      mPath = path;
      mCmd = cmd;
      mCommandType = commandType;
      mArgs = args;
    }

    /**
     * @return the subcommand
     */
    public String getCommand() {
      return mCmd;
    }

    /**
     * @return the path
     */
    public AlluxioURI getPath() {
      return mPath;
    }

    /**
     * @return the cmd args
     */
    public String getArgs() {
      return mArgs;
    }

    /**
     * @return the command type defined in CommandType
     */
    public String getCommandType() {
      return mCommandType;
    }
  }
}
