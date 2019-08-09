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

import java.io.IOException;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link InputFile} implementation using the Alluxio {@link FileSystem} API.
 *
 */
public class AlluxioInputFile implements InputFile {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioInputFile.class);

  private final FileSystem mFileSystem;
  private final AlluxioURI mPath;
  private Long mLength;

  public static InputFile fromPath(FileSystem fs, String path) {
    return new AlluxioInputFile(fs, path);
  }

  private AlluxioInputFile(FileSystem fs, String path) {
    mFileSystem = fs;
    mPath = new AlluxioURI(path);
  }

  @Override
  public long getLength() {
    if (mLength == null) {
      try {
        mLength = mFileSystem.getStatus(mPath).getLength();
      } catch (IOException e) {
        LOG.debug("IOException encountered trying to get length for file {} \n {}", mPath, e);
        return 0;
      } catch (AlluxioException e) {
        LOG.debug("AlluxioException encountered trying to get length for file {} \n {}", mPath, e);
        return 0;
      }
    }
    return mLength;
  }

  @Override
  public SeekableInputStream newStream() {
    SeekableInputStream stream;

    try {
      stream = AlluxioStreams.wrap(mFileSystem.openFile(mPath));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", mPath);
    } catch (AlluxioException e) {
      LOG.debug("AlluxioException encountered trying to get length for file {} \n {}", mPath, e);
      throw new RuntimeException(e);
    }

    return stream;
  }

  @Override
  public String location() {
    return mPath.toString();
  }

  @Override
  public boolean exists() {
    try {
      return mFileSystem.exists(mPath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to check existence for file: " + mPath, e);
    }
  }

  @Override
  public String toString() {
    return mPath.toString();
  }
}
