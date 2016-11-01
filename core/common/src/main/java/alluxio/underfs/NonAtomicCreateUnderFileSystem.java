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
import alluxio.security.authorization.Permission;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.NonAtomicCreateOptions;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link UnderFileSystem} which does not have an atomic create operation, i.e. the file in
 * creation may show up in listings before its closed and partial contents can be read.
 */
public abstract class NonAtomicCreateUnderFileSystem extends UnderFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link NonAtomicCreateUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public NonAtomicCreateUnderFileSystem(AlluxioURI uri) {
    super(uri);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    String temporaryPath = PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(), path);

    return new NonAtomicFileOutputStream(createNonAtomic(temporaryPath, options), this,
        new NonAtomicCreateOptions(temporaryPath, path, options));
  }

  /**
   * Complete create operation by renaming temporary path to permanent.
   *
   * @param options information to complete create
   * @throws IOException when rename fails
   */
  public void completeCreate(NonAtomicCreateOptions options) throws IOException {
    String temporaryPath = options.getTemporaryPath();
    String permanentPath = options.getPermanentPath();
    if (!rename(temporaryPath, permanentPath)) {
      if (!delete(temporaryPath, false)) {
        LOG.error("Failed to delete temporary file {}", temporaryPath);
      }
      throw new IOException(
          ExceptionMessage.FAILED_UFS_RENAME.getMessage(temporaryPath, permanentPath));
    }

    // Rename does not preserve permissions
    Permission perm = options.getCreateOptions().getPermission();
    if (!perm.getOwner().isEmpty() || !perm.getGroup().isEmpty()) {
      try {
        setOwner(permanentPath, perm.getOwner(), perm.getGroup());
      } catch (Exception e) {
        LOG.warn("Failed to update the ufs ownership, default values will be used. " + e);
      }
    }
    // TODO(chaomin): consider setMode of the ufs file.
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
