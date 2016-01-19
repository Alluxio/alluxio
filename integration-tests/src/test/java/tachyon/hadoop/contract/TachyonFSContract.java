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

package tachyon.hadoop.contract;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import tachyon.Constants;
import tachyon.hadoop.TFS;
import tachyon.master.LocalTachyonCluster;

/**
 * This class provides an implementation of {@link AbstractFSContract} using
 * {@link tachyon.hadoop.TFS}. This will be used to run Hadoop Contract tests which verify the
 * {@link tachyon.hadoop.AbstractTFS} interface.
 * More information can be found here:
 * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/testing.html
 */
public class TachyonFSContract extends AbstractFSContract {

  public static final String CONTRACT_XML = "contract/tachyonfs.xml";
  public static final String SYSPROP_TEST_BUILD_DATA = "test.build.data";
  public static final String DEFAULT_TEST_BUILD_DATA_DIR = "test/build/data";
  private final LocalTachyonCluster mLocalTachyonCluster;
  private FileSystem mFS;

  /**
   * Creates a new {@link TachyonFSContract}.
   *
   * @param conf configuration for hdfs
   * @param cluster the Tachyon cluster to test in this contract
   */
  public TachyonFSContract(Configuration conf, LocalTachyonCluster cluster) {
    super(conf);
    mLocalTachyonCluster = cluster;
    //insert the base features
    addConfResource(getContractXml());
  }

  /**
   * Return the contract file for this filesystem
   * @return the XML
   */
  protected String getContractXml() {
    return CONTRACT_XML;
  }

  @Override
  public void init() throws IOException {
    super.init();
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());

    URI uri = URI.create(mLocalTachyonCluster.getMasterUri());

    mFS = FileSystem.get(uri, conf);
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return mFS;
  }

  @Override
  public String getScheme() {
    return Constants.SCHEME;
  }

  @Override
  public Path getTestPath() {
    Path path = mFS.makeQualified(new Path(getTestDataDir()));
    return path;
  }

  /**
   * Get the test data directory
   * @return the directory for test data
   */
  protected String getTestDataDir() {
    return System.getProperty(SYSPROP_TEST_BUILD_DATA, DEFAULT_TEST_BUILD_DATA_DIR);
  }
}
