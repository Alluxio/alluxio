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

package alluxio.master.catalog;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AlluxioFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFileIO.class);
  private final FileSystem mFileSystem;
  public AlluxioFileIO(FileSystem fs) {
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
      mFileSystem.delete(new AlluxioURI(path));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "unable to delete %s", path);
    } catch (AlluxioException e) {
      e.printStackTrace();
    }
  }
}
