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

package alluxio.cli.command.metadatacache;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.metadata.FuseURIStatus;

/**
 * The metadata cache 'drop' subcommand.
 */
public final class DropCommand extends AbstractMetadataCacheSubCommand {

  /**
   * @param fs the file system the command takes effect on
   * @param fuseFsOpts the options for AlluxioFuse filesystem
   * @param parentCommandName the parent command name
   */
  public DropCommand(FileSystem fs, AlluxioFuseFileSystemOpts fuseFsOpts,
      String parentCommandName) {
    super(fs, fuseFsOpts, parentCommandName);
  }

  @Override
  public String getCommandName() {
    return "drop";
  }

  @Override
  public String getUsage() {
    return String.format("%s%s%s.%s.%s", Constants.DEAFULT_FUSE_MOUNT,
        "/<path to be cleaned>", Constants.ALLUXIO_CLI_PATH, getParentCommandName(),
        getCommandName());
  }

  @Override
  protected FuseURIStatus runSubCommand(AlluxioURI path, String [] argv,
      MetadataCachingBaseFileSystem fileSystem) {
    fileSystem.dropMetadataCache(path);
    return FuseURIStatus.newBuilder().setCompleted(true).build();
  }

  @Override
  public String getDescription() {
    return "Clear the metadata cache of a path and its parents."
        + "If the given path is a directory, "
        + "clear the metadata cache of all it's children recursively";
  }
}
