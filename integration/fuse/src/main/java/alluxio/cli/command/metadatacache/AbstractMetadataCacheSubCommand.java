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
import alluxio.cli.command.AbstractFuseShellCommand;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.FuseMetadataCache;

/**
 * Metadata cache sub command.
 */
public abstract class AbstractMetadataCacheSubCommand extends AbstractFuseShellCommand {

  /**
   * @param fileSystem   the file system the command takes effect on
   * @param fuseFsOpts   Alluxio configuration
   * @param commandName  the parent command name
   */
  public AbstractMetadataCacheSubCommand(FileSystem fileSystem,
      AlluxioFuseFileSystemOpts fuseFsOpts, String commandName) {
    super(fileSystem, fuseFsOpts, commandName);
  }

  @Override
  public FuseMetadataCache.FuseURIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {
    if (!mFuseFsOpts.isMetadataCacheEnabled()) {
      throw new InvalidArgumentException(String.format("%s command is "
              + "not supported when %s is false", getCommandName(),
          PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
    }
    return runSubCommand(path, argv, (MetadataCachingBaseFileSystem) mFileSystem);
  }

  /**
   * Run the metadatacache subcommand.
   *
   * @return the result of running the command
   */
  protected abstract FuseMetadataCache.FuseURIStatus runSubCommand(AlluxioURI path, String[] argv,
      MetadataCachingBaseFileSystem fs);
}
