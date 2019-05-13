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

package alluxio.cli;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.examples.S3ASpecificOperations;
import alluxio.examples.UnderFileSystemCommonOperations;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.gcs.GCSUnderFileSystemFactory;
import alluxio.underfs.hdfs.HdfsUnderFileSystemFactory;
import alluxio.underfs.kodo.KodoUnderFileSystemFactory;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.oss.OSSUnderFileSystemFactory;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.underfs.s3a.S3AUnderFileSystemFactory;
import alluxio.underfs.swift.SwiftUnderFileSystemFactory;
import alluxio.underfs.wasb.WasbUnderFileSystemFactory;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for Alluxio under filesystems. It describes the contract of Alluxio
 * with the UFS through the UFS interface.
 */
public final class AlluxioUnderFileSystemContractTest {
  private static final Logger LOG
      = LoggerFactory.getLogger(AlluxioUnderFileSystemContractTest.class);

  @Parameter(names = {"-n", "--name"}, required = true,
      description = "Name of the under filesystem.")
  private String mUfsName;

  @Parameter(names = {"-p", "--path"}, required = true,
      description = "The under filesystem path to run tests against.")
  private String mUfsPath;

  @Parameter(names = {"-h", "--help"}, help = true)
  private boolean mHelp = false;

  private String mUnderfsAddress;
  private UnderFileSystem mUfs;
  private UnderFileSystemFactory mFactory;
  private Set<String> mValidUfsName;

  private AlluxioUnderFileSystemContractTest() {
    mValidUfsName = new HashSet<>(Arrays
        .asList(new String[] {"HDFS", "S3A", "GCS", "KODO", "OSS", "SWIFT", "WASB"}));
  }

  private void run() throws Exception {
    // Check if the ufs name is valid
    if (mUfsName.isEmpty() || !mValidUfsName.contains(mUfsName)) {
      LOG.error("The given under filesystem name is invalid. Valid name includes "
          + StringUtils.join(mValidUfsName, ","));
      System.exit(1);
    }

    // Check if the ufs path is valid
    createUnderFileSystemFactory();
    if (mFactory == null || !mFactory.supportsPath(mUfsPath)) {
      LOG.error("%s is not a valid %s path", mUfsPath, mUfsName);
      System.exit(1);
    }

    runCommonOperations();
    if (mUfsName.equals("S3A")) {
      runS3AOperations();
    }
    CliUtils.printPassInfo(true);
  }

  private void runCommonOperations() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.UNDERFS_LISTING_LENGTH, "50");
    conf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
    // Increase the buffer time of journal writes to speed up tests
    conf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");

    createUnderFileSystem(conf);
    UnderFileSystemCommonOperations ops
        = new UnderFileSystemCommonOperations(mUfsPath, mUnderfsAddress, mUfs, conf);
    try {
      ops.runTests();
    } catch (Throwable t) {
      if (mUfsName.equals("S3A")) {
        LOG.info("Related S3 operations: "
            + StringUtils.join(ops.getRelatedS3AOperations(), ","));
      }
    } finally {
      cleanup();
    }
  }

  private void runS3AOperations() throws IOException {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1MB");
    conf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");
    conf.set(PropertyKey.UNDERFS_S3A_LIST_OBJECTS_VERSION_1, "true");
    conf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_ENABLED, "true");
    conf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE, "5MB");
    conf.set(PropertyKey.UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE, "0");

    createUnderFileSystem(conf);
    try {
      S3ASpecificOperations ops
          = new S3ASpecificOperations(mUfsPath, mUnderfsAddress, (S3AUnderFileSystem) mUfs, conf);
      ops.runTests();
    } finally {
      cleanup();
    }
  }

  private void createUnderFileSystem(InstancedConfiguration conf) {
    mUnderfsAddress = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    mUfs = mFactory.create(mUfsPath, UnderFileSystemConfiguration.defaults(conf), conf);
    if (mUfs == null) {
      LOG.error("Failed to create under filesystem");
      System.exit(1);
    }
  }

  private void cleanup() throws IOException {
    mUfs.deleteDirectory(mUnderfsAddress, DeleteOptions.defaults().setRecursive(true));
    mUfs.close();
  }

  private void createUnderFileSystemFactory() {
    switch (mUfsName) {
      case "HDFS":
        mFactory = new HdfsUnderFileSystemFactory();
        break;
      case "S3A":
        mFactory = new S3AUnderFileSystemFactory();
        break;
      case "GCS":
        mFactory = new GCSUnderFileSystemFactory();
        break;
      case "KODO":
        mFactory = new KodoUnderFileSystemFactory();
        break;
      case "OSS":
        mFactory = new OSSUnderFileSystemFactory();
        break;
      case "SWIFT":
        mFactory = new SwiftUnderFileSystemFactory();
        break;
      case "WASB":
        mFactory = new WasbUnderFileSystemFactory();
        break;
      default:
        // do nothing;
    }
  }

  /**
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    AlluxioUnderFileSystemContractTest test = new AlluxioUnderFileSystemContractTest();
    JCommander jc = new JCommander(test);
    jc.setProgramName(AlluxioUnderFileSystemContractTest.class.getName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      jc.usage();
      LOG.info(getHelpMessage());
      System.exit(1);
    }
    if (test.mHelp) {
      jc.usage();
      LOG.info(getHelpMessage());
    } else {
      test.run();
    }
  }

  private static String getHelpMessage() {
    return "Test description:\n"
        + "Test the integration between Alluxio and the under filesystem. "
        + "If the given under filesystem name is S3A, this test can also be used as "
        + "a S3A compatibility test to test if the target under filesystem can "
        + "fulfill the minimum S3A compatibility requirements in order to "
        + "work well with Alluxio through Alluxio's integration with S3A. \n"
        + "Command line example: 'bin/alluxio runUnderFileSystemTest --name S3A "
        + "--path=s3a://testPath -Daws.accessKeyId=<accessKeyId> -Daws.secretKeyId=<secretKeyId>"
        + "-Dalluxio.underfs.s3.endpoint=<endpoint_url> "
        + "-Dalluxio.underfs.s3.disable.dns.buckets=true'";
  }
}
