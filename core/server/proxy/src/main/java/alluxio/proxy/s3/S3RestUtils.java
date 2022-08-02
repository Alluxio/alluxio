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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.proto.journal.File;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.ServerUserState;
import alluxio.util.SecurityUtils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Utilities for handling S3 REST calls.
 */
public final class S3RestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestUtils.class);

  public static final String MULTIPART_UPLOADS_METADATA_DIR = AlluxioURI.SEPARATOR
      + S3Constants.S3_METADATA_ROOT_DIR + AlluxioURI.SEPARATOR
      + S3Constants.S3_METADATA_UPLOADS_DIR;

  /**
   * Calls the given {@link S3RestUtils.RestCallable} and handles any exceptions thrown.
   *
   * @param <T> the return type of the callable
   * @param resource the resource (bucket or object) to be operated on
   * @param callable the callable to call
   * @return the response object
   */
  public static <T> Response call(String resource, S3RestUtils.RestCallable<T> callable) {
    try {
      // TODO(cc): reconsider how to enable authentication
      if (SecurityUtils.isSecurityEnabled(ServerConfiguration.global())
              && AuthenticatedClientUser.get(ServerConfiguration.global()) == null) {
        AuthenticatedClientUser.set(ServerUserState.global().getUser().getName());
      }
    } catch (IOException e) {
      LOG.warn("Failed to set AuthenticatedClientUser in REST service handler: {}", e.toString());
      return S3ErrorResponse.createErrorResponse(new S3Exception(
          e, resource, S3ErrorCode.INTERNAL_ERROR), resource);
    }

    try {
      T result = callable.call();
      if (result == null) {
        return Response.ok().build();
      }
      if (result instanceof Response) {
        return (Response) result;
      }
      if (result instanceof Response.Status) {
        switch ((Response.Status) result) {
          case OK:
            return Response.ok().build();
          case ACCEPTED:
            return Response.accepted().build();
          case NO_CONTENT:
            return Response.noContent().build();
          default:
            return S3ErrorResponse.createErrorResponse(new S3Exception(resource,
                new S3ErrorCode(S3ErrorCode.INTERNAL_ERROR.getCode(),
                    "Response status is invalid",
                    S3ErrorCode.INTERNAL_ERROR.getStatus())), resource);
        }
      }
      // Need to explicitly encode the string as XML because Jackson will not do it automatically.
      XmlMapper mapper = new XmlMapper();
      return Response.ok(mapper.writeValueAsString(result)).build();
    } catch (Exception e) {
      LOG.warn("Error invoking REST endpoint for {}:\n{}", resource, e.getMessage());
      return S3ErrorResponse.createErrorResponse(e, resource);
    }
  }

  /**
   * An interface representing a callable.
   *
   * @param <T> the return type of the callable
   */
  public interface RestCallable<T> {
    /**
     * The REST endpoint implementation.
     *
     * @return the return value from the callable
     */
    T call() throws S3Exception;
  }

  /**
   * @param bucketPath the bucket path like "/bucket", "/mount/point/bucket"
   * @param objectKey  the object key like "img/2017/9/1/s3.jpg"
   * @param uploadId   the upload ID
   * @return the temporary directory used to hold parts of the object during multipart uploads
   */
  public static String getMultipartTemporaryDirForObject(
      String bucketPath, String objectKey, String uploadId) {
    return bucketPath + AlluxioURI.SEPARATOR + objectKey
        + "_" + uploadId;
  }

  /**
   * @param uploadId the upload ID
   * @return the Alluxio UFS filepath containing the metadata for this upload
   */
  public static String getMultipartMetaFilepathForUploadId(String uploadId) {
    return MULTIPART_UPLOADS_METADATA_DIR + AlluxioURI.SEPARATOR + uploadId;
  }

  /**
   * @param epoch the milliseconds from the epoch
   * @return the string representation of the epoch in the S3 date format
   */
  public static String toS3Date(long epoch) {
    final DateFormat s3DateFormat = new SimpleDateFormat(S3Constants.S3_DATE_FORMAT_REGEXP);
    return s3DateFormat.format(new Date(epoch));
  }

  /**
   * Format bucket path.
   *
   * @param bucketPath bucket path
   * @return bucket path after format
   */
  public static String parsePath(String bucketPath) {
    // Normalize the bucket by replacing ":" with "/"
    return bucketPath.replace(S3Constants.BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource complete bucket path
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toBucketS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.BUCKET_NOT_EMPTY);
    } catch (FileAlreadyExistsException e) {
      return new S3Exception(e, resource, S3ErrorCode.BUCKET_ALREADY_EXISTS);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_BUCKET);
    } catch (InvalidPathException e) {
      return new S3Exception(e, resource, S3ErrorCode.INVALID_BUCKET_NAME);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Check if a path in alluxio is a directory.
   *
   * @param fs instance of {@link FileSystem}
   * @param bucketPath bucket complete path
   */
  public static void checkPathIsAlluxioDirectory(FileSystem fs, String bucketPath)
      throws S3Exception {
    try {
      URIStatus status = fs.getStatus(new AlluxioURI(bucketPath));
      if (!status.isFolder()) {
        throw new InvalidPathException("Bucket " + bucketPath
            + " is not a valid Alluxio directory.");
      }
    } catch (Exception e) {
      throw toBucketS3Exception(e, bucketPath);
    }
  }

  /**
   * Fetches and returns the corresponding {@link URIStatus} for both
   * the multipart upload temp directory and the Alluxio S3 metadata file.
   * @param fs instance of {@link FileSystem}
   * @param multipartTempDirUri multipart upload tmp directory URI
   * @param uploadId multipart upload Id
   * @return a list of file statuses:
   *         - first, the multipart upload temp directory
   *         - second, the metadata file
   */
  public static List<URIStatus> checkStatusesForUploadId(
      FileSystem fs, AlluxioURI multipartTempDirUri, String uploadId)
      throws AlluxioException, IOException {
    // Verify the multipart upload dir exists and is a folder
    URIStatus multipartTempDirStatus = fs.getStatus(multipartTempDirUri);
    if (!multipartTempDirStatus.isFolder()) {
      //TODO(czhu): determine intended behavior in this edge-case
      throw new RuntimeException(
          "Alluxio UFS contains a file at intended multipart-upload directory path: "
              + multipartTempDirUri);
    }

    // Verify the multipart upload meta file exists and matches the file ID
    final AlluxioURI metaUri = new AlluxioURI(
        S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId));
    URIStatus metaStatus = fs.getStatus(metaUri);
    if (metaStatus.getXAttr() == null
        || !metaStatus.getXAttr().containsKey(S3Constants.UPLOADS_FILE_ID_XATTR_KEY)) {
      //TODO(czhu): determine intended behavior in this edge-case
      throw new RuntimeException(
          "Alluxio is missing multipart-upload metadata for upload ID: " + uploadId);
    }
    if (Longs.fromByteArray(metaStatus.getXAttr().get(S3Constants.UPLOADS_FILE_ID_XATTR_KEY))
        != multipartTempDirStatus.getFileId()) {
      throw new RuntimeException(
          "Alluxio mismatched file ID for multipart-upload with upload ID: " + uploadId);
    }
    return new ArrayList<>(Arrays.asList(multipartTempDirStatus, metaStatus));
  }

  /**
   * Delete an existing key.
   *
   * @param fs instance of {@link FileSystem}
   * @param objectURI the key uri
   */
  public static void deleteExistObject(final FileSystem fs, AlluxioURI objectURI)
      throws IOException, AlluxioException {
    deleteExistObject(fs, objectURI, false);
  }

  /**
   * Delete an existing key.
   *
   * @param fs instance of {@link FileSystem}
   * @param objectURI the key uri
   * @param recursive if delete option is recursive
   */
  public static void deleteExistObject(final FileSystem fs, AlluxioURI objectURI, Boolean recursive)
      throws IOException, AlluxioException {
    if (fs.exists(objectURI)) {
      if (recursive) {
        fs.delete(objectURI, DeletePOptions.newBuilder().setRecursive(true).build());
      } else {
        fs.delete(objectURI);
      }
      LOG.info("Remove exist object: {} for overwrite.", objectURI.getPath());
    }
  }

  /**
   * @return s3 WritePType
   */
  public static WritePType getS3WriteType() {
    return ServerConfiguration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class).toProto();
  }

  /**
   * Given xAttr, parses and returns the Content-Type header metadata
   * as its corresponding {@link MediaType}, or otherwise defaults
   * to {@link MediaType#APPLICATION_OCTET_STREAM_TYPE}.
   * @param xAttr the Inode's xAttrs
   * @return the {@link MediaType} corresponding to the Content-Type header
   */
  public static MediaType deserializeContentType(Map<String, byte[]> xAttr) {
    MediaType type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
    // Fetch the Content-Type from the Inode xAttr
    if (xAttr == null) {
      return type;
    }
    if (xAttr.containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
      String contentType = new String(xAttr.get(
          S3Constants.CONTENT_TYPE_XATTR_KEY), S3Constants.HEADER_CHARSET);
      if (!contentType.isEmpty()) {
        type = MediaType.valueOf(contentType);
      }
    }
    return type;
  }

  /**
   * Given xAttr, parses and deserializes the Tagging metadata
   * into a {@link TaggingData} object. Returns null if no data exists.
   * @param xAttr the Inode's xAttrs
   * @return the deserialized {@link TaggingData} object
   */
  public static TaggingData deserializeTags(Map<String, byte[]> xAttr)
      throws IOException {
    // Fetch the S3 tags from the Inode xAttr
    if (xAttr == null || !xAttr.containsKey(S3Constants.TAGGING_XATTR_KEY)) {
      return null;
    }
    return TaggingData.deserialize(xAttr.get(S3Constants.TAGGING_XATTR_KEY));
  }

  /**
   * Given a URL-encoded Tagging header, parses and deserializes the Tagging metadata
   * into a {@link TaggingData} object. Returns null on empty strings.
   * @param taggingHeader the URL-encoded Tagging header
   * @param maxHeaderMetadataSize Header user-metadata size limit validation (default max: 2 KB)
   * - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
   * @return the deserialized {@link TaggingData} object
   */
  public static TaggingData deserializeTaggingHeader(
      String taggingHeader, int maxHeaderMetadataSize) throws S3Exception {
    if (taggingHeader == null || taggingHeader.isEmpty()) { return null; }
    if (maxHeaderMetadataSize > 0
        && taggingHeader.getBytes(S3Constants.TAGGING_CHARSET).length
        > maxHeaderMetadataSize) {
      throw new S3Exception(S3ErrorCode.METADATA_TOO_LARGE);
    }
    Map<String, String> tagMap = new HashMap<>();
    for (String tag : taggingHeader.split("&")) {
      String[] entries = tag.split("=");
      if (entries.length > 1) {
        tagMap.put(entries[0], entries[1]);
      } else { // Key was provided without a value
        tagMap.put(entries[0], "");
      }
    }
    return new TaggingData().addTags(tagMap);
  }

  /**
   * This helper method is used to set the ETag xAttr on an object.
   * @param fs The {@link FileSystem} used to make the gRPC request
   * @param objectUri The {@link AlluxioURI} for the object to update
   * @param entityTag The entity tag of the object (MD5 checksum of the object contents)
   * @throws IOException
   * @throws AlluxioException
   */
  public static void setEntityTag(FileSystem fs, AlluxioURI objectUri, String entityTag)
      throws IOException, AlluxioException {
    fs.setAttribute(objectUri, SetAttributePOptions.newBuilder()
        .putXattr(S3Constants.ETAG_XATTR_KEY,
            ByteString.copyFrom(entityTag, S3Constants.XATTR_STR_CHARSET))
        .setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
        .build());
  }

  /**
   * Comparator based on uri name， treat uri name as a Long number.
   */
  public static class URIStatusNameComparator implements Comparator<URIStatus>, Serializable {

    private static final long serialVersionUID = 733270188584155565L;

    @Override
    public int compare(URIStatus o1, URIStatus o2) {
      long part1 = Long.parseLong(o1.getName());
      long part2 = Long.parseLong(o2.getName());
      return Long.compare(part1, part2);
    }
  }

  private S3RestUtils() {} // prevent instantiation
}
