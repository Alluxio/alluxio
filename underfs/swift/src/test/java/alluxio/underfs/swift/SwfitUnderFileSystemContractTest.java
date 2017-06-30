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

package alluxio.underfs.swift;

import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;

/**
 * This UFS contract test will use Swift as the backing store.
 */
public final class SwfitUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String INTEGRATION_SWIFT_CONTAINER_KEY = "containerKey";

  private String mSwiftContainer;

  public SwfitUnderFileSystemContractTest() {
    String swiftContainer = System.getProperty(INTEGRATION_SWIFT_CONTAINER_KEY);
    Preconditions.checkState(swiftContainer != null);
    Preconditions.checkState(new SwiftUnderFileSystemFactory().supportsPath(swiftContainer));
    mSwiftContainer = swiftContainer;
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new SwiftUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return mSwiftContainer;
  }
}
