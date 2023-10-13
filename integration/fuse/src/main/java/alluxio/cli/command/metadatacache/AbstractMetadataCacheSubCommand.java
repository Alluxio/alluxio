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
import alluxio.client.file.MetadataCachingFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.status.InvalidArgumentException;

/**
 * Metadata cache sub command.
 */
public abstract class AbstractMetadataCacheSubCommand extends AbstractFuseShellCommand {

  /**
   * @param fileSystem   the file system the command takes effect on
   * @param conf the Alluxio configuration
   * @param commandName  the parent command name
   */
  public AbstractMetadataCacheSubCommand(FileSystem fileSystem,
      AlluxioConfiguration conf, String commandName) {
    super(fileSystem, conf, commandName);
  }

  @Override
  public URIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {
    if (!mConf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED)) {
      throw new InvalidArgumentRuntimeException(String.format("%s command is "
              + "not supported when %s is false", getCommandName(),
          PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
    }
    return runSubCommand(path, argv, findMetadataCachingFileSystem());
  }

  /**
   * Run the metadatacache subcommand.
   *
   * @return the result of running the command
   */
  protected abstract URIStatus runSubCommand(AlluxioURI path, String[] argv,
      MetadataCachingFileSystem fs);

  /**
   * Find MetadataCachingFileSystem by given filesystem.
   */
  private MetadataCachingFileSystem findMetadataCachingFileSystem() {
    if (mFileSystem instanceof MetadataCachingFileSystem) {
      return (MetadataCachingFileSystem) mFileSystem;
    }
    if (mFileSystem instanceof LocalCacheFileSystem) {
      FileSystem underlyingFileSystem = ((LocalCacheFileSystem) mFileSystem)
          .getUnderlyingFileSystem();
      if (underlyingFileSystem instanceof MetadataCachingFileSystem) {
        return (MetadataCachingFileSystem) underlyingFileSystem;
      } else {
        throw new IllegalStateException(
            "The expected underlying FileSystem of LocalCacheFileSystem "
                + "is MetadataCachingFileSystem, but found "
                + mFileSystem.getClass().getSimpleName());
      }
    }
    throw new IllegalStateException(
        String.format("The expected FileSystem is %s or %s, but found %s",
            MetadataCachingFileSystem.class.getSimpleName(),
            LocalCacheFileSystem.class.getSimpleName(), mFileSystem.getClass().getSimpleName()));
  }
}
