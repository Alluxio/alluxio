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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base class of an Alluxio Test Case.
 */
@ThreadSafe
public class AlluxioTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioTestCase.class);

  private final FileSystemContext mFsContext;

  private final String mDir;

  private Map<String, Function<FileSystem, Boolean>> mAllSubTests;

  /**
   * @param dir
   * @param fsContext the {@link FileSystemContext } to use for client operations
   */
  public AlluxioTestCase(String dir, FileSystemContext fsContext) {
    mDir = dir;
    mFsContext = fsContext;
    mAllSubTests = new HashMap<>();
  }

  /**
   * Get dir.
   *
   * @return Dir
   */
  public String getDir() {
    return this.mDir;
  }

  /**
   * Get File System Context.
   *
   * @return File System Context
   */
  public FileSystemContext getFsContext() {
    return this.mFsContext;
  }

  /**
   * Get list of all sub test cases.
   * @return a map of all sub test cases with their names
   */
  public Map<String, Function<FileSystem, Boolean>> getAllSubTests() {
    return this.mAllSubTests;
  }

  /**
   * Adding a single sub test case.
   * This method can be called in the constructor of a child class to add sub test cases.
   *
   * @param name     name of this sub test case
   * @param testCase function to do the sub test
   */
  public void addSubTestCase(String name, Function<FileSystem, Boolean> testCase) {
    mAllSubTests.put(name, testCase);
  }
}
