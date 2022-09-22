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
import alluxio.client.file.MetadataCachingFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.wire.FileInfo;

/**
 * The metadata cache 'size' subcommand.
 */
public final class SizeCommand extends AbstractMetadataCacheSubCommand {

  /**
   * @param fs the file system the command takes effect on
   * @param fuseFsOpts the options for AlluxioFuse filesystem
   * @param parentCommandName the parent command name
   */
  public SizeCommand(FileSystem fs, AlluxioFuseFileSystemOpts fuseFsOpts,
      String parentCommandName) {
    super(fs, fuseFsOpts, parentCommandName);
  }

  @Override
  public String getCommandName() {
    return "size";
  }

  @Override
  public String getUsage() {
    return String.format("%s%s.%s.%s", Constants.DEAFULT_FUSE_MOUNT,
        Constants.ALLUXIO_CLI_PATH, getParentCommandName(), getCommandName());
  }

  @Override
  protected URIStatus runSubCommand(AlluxioURI path, String [] argv,
      MetadataCachingFileSystem fileSystem) {
    // The 'ls -l' command will show metadata cache size in the <filesize> field.
    long size = fileSystem.getMetadataCacheSize();
    return new URIStatus(new FileInfo().setLength(size).setCompleted(true));
  }

  @Override
  public String getDescription() {
    return "Get fuse client metadata size.";
  }
}
