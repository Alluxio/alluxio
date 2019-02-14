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

package alluxio.client.hadoop.contract;

import alluxio.Constants;
import alluxio.hadoop.FileSystem;
import alluxio.master.LocalAlluxioCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import java.io.IOException;
import java.net.URI;

/**
 * This class provides an implementation of {@link AbstractFSContract} using {@link FileSystem}.
 * This will be used to run Hadoop Contract tests which verify the
 * {@link alluxio.hadoop.AbstractFileSystem} interface. More information can be found here:
 * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/testing.html
 */
public class FileSystemContract extends AbstractFSContract {

  public static final String CONTRACT_XML = "contract/alluxiofs.xml";
  public static final String SYSPROP_TEST_BUILD_DATA = "test.build.data";
  public static final String DEFAULT_TEST_BUILD_DATA_DIR = "test/build/data";
  private final LocalAlluxioCluster mLocalAlluxioCluster;
  private org.apache.hadoop.fs.FileSystem mFS;
  private Configuration mHadoopConf;

  /**
   * Creates a new {@link FileSystemContract}.
   *
   * @param conf configuration for hdfs
   * @param cluster the Alluxio cluster to test in this contract
   */
  public FileSystemContract(Configuration conf, LocalAlluxioCluster cluster) {
    super(conf);
    mHadoopConf = conf;
    mLocalAlluxioCluster = cluster;
    //insert the base features
    addConfResource(getContractXml());
  }

  /**
   * Return the contract file for this filesystem.
   *
   * @return the XML
   */
  protected String getContractXml() {
    return CONTRACT_XML;
  }

  @Override
  public void init() throws IOException {
    super.init();
    mHadoopConf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create(mLocalAlluxioCluster.getMasterURI());

    mFS = org.apache.hadoop.fs.FileSystem.get(uri, mHadoopConf);
  }

  @Override
  public org.apache.hadoop.fs.FileSystem getTestFileSystem() throws IOException {
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
   * Get the test data directory.
   *
   * @return the directory for test data
   */
  protected String getTestDataDir() {
    return System.getProperty(SYSPROP_TEST_BUILD_DATA, DEFAULT_TEST_BUILD_DATA_DIR);
  }
}
