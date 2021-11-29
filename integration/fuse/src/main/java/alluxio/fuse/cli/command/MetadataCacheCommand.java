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

package alluxio.fuse.cli.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCachingBaseFileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets information of or makes changes to the Fuse client metadata cache.
 */
@ThreadSafe
public final class MetadataCacheCommand {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheCommand.class);

  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;

  public MetadataCacheCommand(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration) {
    mFileSystem = fileSystem;
    mConf = alluxioConfiguration;
  }

  public URIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {
    if (!mConf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED)) {
      throw new UnsupportedOperationException(String
          .format("metadatacache command is not supported when %s is false",
              PropertyKey.USER_METADATA_CACHE_ENABLED.getName()));
    }
    if (argv.length > 1) {
      throw new InvalidArgumentException("metadatacache requires 0 or 1 arguments");
    }
    if (argv.length == 0) {
      return getMetadataCacheSize();
    }
    String cmd = argv[0];
    switch (cmd) {
      // TODO(lu) better organizing sub command names
      case "dropAll":
        ((MetadataCachingBaseFileSystem) mFileSystem).dropMetadataCacheAll();
        return new URIStatus(new FileInfo().setCompleted(true));
      case "drop":
        ((MetadataCachingBaseFileSystem) mFileSystem).dropMetadataCache(path);
        return new URIStatus(new FileInfo().setCompleted(true));
      case "size":
      default:
        return getMetadataCacheSize();
    }
  }

  private URIStatus getMetadataCacheSize() {
    // The 'ls -al' command will show metadata cache size in the <filesize> field.
    long size = ((MetadataCachingBaseFileSystem) mFileSystem).getMetadataCacheSize();
    return new URIStatus(new FileInfo().setLength(size).setCompleted(true));
  }
}
