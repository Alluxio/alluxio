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

package alluxio.worker.task;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.Route;
import alluxio.grpc.WriteOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * ValidateHandler is responsible for validating files at source and destination.
 */
public class ValidateHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ValidateHandler.class);
  private static final GetStatusPOptions GET_STATUS_OPTIONS =
      GetStatusPOptions.getDefaultInstance().toBuilder().setIncludeRealContentHash(true).build();

  /**
   * Validates a file from source to destination.
   *
   * @param route        the route
   * @param writeOptions the write options
   * @param srcFs        the source file system
   * @param dstFs        the destination file system
   * @return true means the validation passes
   *         false means the file is already in the target UFS
   */
  public static boolean validate(Route route, WriteOptions writeOptions,
                          FileSystem srcFs, FileSystem dstFs) {

    AlluxioURI src = new AlluxioURI(route.getSrc());
    AlluxioURI dst = new AlluxioURI(route.getDst());
    URIStatus dstStatus = null;
    URIStatus sourceStatus;
    try {
      dstStatus = dstFs.getStatus(dst, GET_STATUS_OPTIONS);
    } catch (FileNotFoundException | NotFoundRuntimeException ignore) {
      // ignored
    } catch (FileDoesNotExistException ignore) {
      // should not happen
    } catch (AlluxioException | IOException e) {
      throw new InternalRuntimeException(e);
    }
    try {
      sourceStatus = srcFs.getStatus(src, GET_STATUS_OPTIONS);
    } catch (Exception e) {
      throw AlluxioRuntimeException.from(e);
    }
    if (dstStatus != null && dstStatus.isFolder() && sourceStatus.isFolder()) {
      // skip if it's already a folder there
      return true;
    }
    if (dstStatus != null && !dstStatus.isFolder() && !writeOptions.getOverwrite()) {
      LOG.debug("File " + route.getDst() + " is already in target UFS");
      return false;
    }
    if (dstStatus != null && (dstStatus.isFolder() != sourceStatus.isFolder())) {
      LOG.debug("Can't replace target because type is not compatible. Target is " + dstStatus
          + ", Source is " + sourceStatus);
      throw new InvalidArgumentRuntimeException(
          "Can't replace target because type is not compatible. Target is " + dstStatus
              + ", Source is " + sourceStatus);
    }
    return true;
  }
}
