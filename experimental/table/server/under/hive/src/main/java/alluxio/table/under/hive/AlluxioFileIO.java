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

package alluxio.table.under.hive;

import alluxio.underfs.UnderFileSystem;

import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Alluxio File IO class.
 */
public class AlluxioFileIO implements FileIO {
  private static final long serialVersionUID = 1507823472921089708L;
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFileIO.class);
  private final UnderFileSystem mFileSystem;

  /**
   * Constructor for AlluxioFileIO.
   *
   * @param fs ufs handle
   */
  public AlluxioFileIO(UnderFileSystem fs) {
    mFileSystem = fs;
  }

  @Override
  public InputFile newInputFile(String path) {
    return AlluxioInputFile.fromPath(mFileSystem, path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return AlluxioOutputFile.fromPath(mFileSystem, path);
  }

  @Override
  public void deleteFile(String path) {
    try {
      mFileSystem.deleteFile(path);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "unable to delete %s", path);
    }
  }
}
