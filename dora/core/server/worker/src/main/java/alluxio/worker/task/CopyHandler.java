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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ErrorType;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.Route;
import alluxio.grpc.WriteOptions;
import alluxio.grpc.WritePType;
import alluxio.underfs.Fingerprint;

import io.grpc.Status;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * CopyHandler is responsible for copying files.
 */
public final class CopyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CopyHandler.class);
  private static final GetStatusPOptions GET_STATUS_OPTIONS =
      GetStatusPOptions.getDefaultInstance().toBuilder().setIncludeRealContentHash(true).build();

  /**
   * Copies a file from source to destination.
   *
   * @param route        the route
   * @param writeOptions the write options
   * @param srcFs        the source file system
   * @param dstFs        the destination file system
   */
  public static void copy(Route route, WriteOptions writeOptions,
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
      // skip copy if it's already a folder there
      return;
    }
    if (dstStatus != null && !dstStatus.isFolder() && !writeOptions.getOverwrite()) {
      throw new FailedPreconditionRuntimeException("File " + route.getDst() + " is already in UFS");
    }
    if (dstStatus != null && (dstStatus.isFolder() != sourceStatus.isFolder())) {
      throw new InvalidArgumentRuntimeException(
          "Can't replace target because type is not compatible. Target is " + dstStatus
              + ", Source is " + sourceStatus);
    }

    if (sourceStatus.isFolder()) {
      try {
        dstFs.createDirectory(dst, CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
        // preserve attributes
        return;
      } catch (Exception e) {
        throw AlluxioRuntimeException.from(e);
      }
    }

    long copiedLength = copyFile(src, dst, srcFs, dstFs, writeOptions.getWriteType());
    if (writeOptions.getCheckContent()) {
      if (!checkLengthAndContentHash(sourceStatus, dst, dstFs, copiedLength)) {
        try {
          dstFs.delete(dst);
        } catch (Exception e) {
          LOG.warn("Failed to delete dst file {} after content mismatch", dst, e);
        }
        throw new AlluxioRuntimeException(Status.FAILED_PRECONDITION, String.format(
            "Copied file %s does not match source %s, there might be concurrent updates to src",
            route.getDst(), route.getSrc()), null, ErrorType.User, true);
      }
    }
  }

  private static long copyFile(AlluxioURI src, AlluxioURI dst, FileSystem srcFs, FileSystem dstFs,
      WritePType writeType) {
    long copiedLength;
    CreateFilePOptions createOptions =
        CreateFilePOptions.getDefaultInstance().toBuilder().setRecursive(true).setMode(
            PMode.newBuilder().setOwnerBits(Bits.ALL).setGroupBits(Bits.ALL)
                 .setOtherBits(Bits.NONE)).setWriteType(writeType).setIsAtomicWrite(true).build();
    try (InputStream in = srcFs.openFile(src);
        OutputStream out = dstFs.createFile(dst, createOptions)) {
      copiedLength = IOUtils.copyLarge(in, out, new byte[Constants.MB * 8]);
    } catch (Exception e) {
      throw new InternalRuntimeException(
          String.format("Exception transmitting i/o stream from %s to %s", src, dst), e);
    }
    return copiedLength;
  }

  private static String parseContentHash(URIStatus sourceStatus) {
    Fingerprint fingerprint = Fingerprint.parse(sourceStatus.getUfsFingerprint());
    if (fingerprint == null) {
      throw new InternalRuntimeException(
          String.format("No fingerprint found for %s", sourceStatus.getPath()));
    }
    String contentHash = fingerprint.getTag(Fingerprint.Tag.CONTENT_HASH);
    if (Objects.equals(contentHash, Fingerprint.UNDERSCORE)) {
      throw new InternalRuntimeException(
          String.format("No content hash found for %s", sourceStatus.getPath()));
    }
    return contentHash;
  }

  private static boolean checkLengthAndContentHash(URIStatus sourceStatus, AlluxioURI dst,
      FileSystem dstFs, long dstLen) {
    if (sourceStatus.getLength() != dstLen) {
      return false;
    }
    //if length==0, we can skip checksum
    if ((sourceStatus.getLength() == 0)) {
      return true;
    }
    else {
      String srcContentHash = parseContentHash(sourceStatus);
      URIStatus dstStatus;
      try {
        dstStatus = dstFs.getStatus(dst, GET_STATUS_OPTIONS);
      } catch (Exception e) {
        throw AlluxioRuntimeException.from(e);
      }
      return srcContentHash.equals(parseContentHash(dstStatus));
    }
  }
}
