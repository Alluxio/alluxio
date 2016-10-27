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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link UnderFileSystem} which does not have an atomic create operation.
 */
public abstract class NonAtomicCreateUnderFileSystem extends UnderFileSystem {

  private final long mNonce;
  private String mTemporaryPath;
  private String mPermanentPath;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link NonAtomicCreateUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public NonAtomicCreateUnderFileSystem(AlluxioURI uri) {
    super(uri);
    mNonce = IdUtils.getRandomNonNegativeLong();
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    mPermanentPath = path;
    mTemporaryPath = PathUtils.temporaryFileName(mNonce, path);

    return new AtomicFileOutputStream(createNonAtomic(mTemporaryPath, options), this);
  }

  /**
   * Complete create operation by renaming temporary path to permanent.
   *
   * @throws IOException when rename fails
   */
  public void completeCreate() throws IOException {
    if (!rename(mTemporaryPath, mPermanentPath)) {
      if (!delete(mTemporaryPath, false)) {
        LOG.error("Failed to delete temporary file {}", mTemporaryPath);
      }
      throw new IOException(
          ExceptionMessage.FAILED_UFS_RENAME.getMessage(mTemporaryPath, mPermanentPath));
    }
  }

  /**
   * Create a non atomic output stream.
   *
   * @param path temporary path
   * @param options the options for create
   * @return a non atomic output stream
   * @throws IOException when create fails
   */
  public abstract OutputStream createNonAtomic(String path, CreateOptions options)
      throws IOException;

}
