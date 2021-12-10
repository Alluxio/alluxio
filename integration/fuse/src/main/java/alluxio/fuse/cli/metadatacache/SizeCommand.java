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

package alluxio.fuse.cli.metadatacache;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.fuse.cli.command.AbstractFuseShellCommand;
import alluxio.wire.FileInfo;

public final class SizeCommand extends AbstractFuseShellCommand {
  public SizeCommand(FileSystem fs, AlluxioConfiguration conf, String parentCommandName) {
    super(fs, conf, parentCommandName);
  }

  @Override
  public String getCommandName() {
    return "size";
  }

  @Override
  public String getUsage() {
    return String.format("ls -l %s%s.%s.%s", Constants.DEAFULT_FUSE_MOUNT,
        Constants.ALLUXIO_CLI_PATH, getParentCommandName(), getCommandName());
  }

  @Override
  public URIStatus run(AlluxioURI path, String [] argv) {
    // The 'ls -l' command will show metadata cache size in the <filesize> field.
    long size = ((MetadataCachingBaseFileSystem) mFileSystem).getMetadataCacheSize();
    return new URIStatus(new FileInfo().setLength(size).setCompleted(true));
  }

  @Override
  public String getDescription() {
    return "Get fuse client metadata size.";
  }

  @Override
  public void validateArgs(String[] argv) throws InvalidArgumentException {
    if (!mConf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED)) {
      throw new UnsupportedOperationException(String.format("metadatacache command is "
              + "not supported when %s is false",
          PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
    }
  }
}
