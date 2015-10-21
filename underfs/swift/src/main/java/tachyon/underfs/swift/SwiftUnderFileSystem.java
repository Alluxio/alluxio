/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.hdfs.HdfsUnderFileSystem;

/**
 * Swift UnderFilesystem implementation
 */
public class SwiftUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public SwiftUnderFileSystem(String fsDefaultName, TachyonConf tachyonConf, Object conf) {
    super(fsDefaultName, tachyonConf, conf);
    LOG.debug("Swift constuctor method");
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }

  /**
   * Prepares the Hadoop configuration necessary to successfully obtain a
   * {@link org.apache.hadoop.fs.FileSystem} instance that can access the provided path
   * <p>
   * Derived implementations that work with specialised Hadoop
   * {@linkplain org.apache.hadoop.fs.FileSystem} API compatible implementations can override this
   * method to add implementation specific configuration necessary for obtaining a usable
   * {@linkplain org.apache.hadoop.fs.FileSystem} instance.
   * </p>
   *
   * @param path File system path
   * @param config Hadoop Configuration
   */
  @Override
  protected void prepareConfiguration(String path, TachyonConf tachyonConf, Configuration config) {
    // To disable the instance cache for hdfs client, otherwise it causes the FileSystem closed
    // exception. Being configurable for unit/integration test only, and not expose to the end-user
    // currently.
    config.set("fs.swift.impl.disable.cache", "true");
    SwiftUnderFileSystemUtils.addKey(config, tachyonConf, Constants.UNDERFS_HDFS_CONFIGURATION);
    config.addResource(new Path(config.get(Constants.UNDERFS_HDFS_CONFIGURATION)));
    super.prepareConfiguration(path, tachyonConf, config);
  }
}
