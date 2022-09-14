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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.proto.journal.File;
import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import alluxio.proxy.s3.signature.AwsSignatureProcessor;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.ServerUserState;
import alluxio.util.SecurityUtils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
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
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
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
      if (SecurityUtils.isSecurityEnabled(Configuration.global())
              && AuthenticatedClientUser.get(Configuration.global()) == null) {
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
            return S3ErrorResponse.createErrorResponse(new S3Exception(
                "Response status is invalid", resource, S3ErrorCode.INTERNAL_ERROR), resource);
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
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource complete bucket path
   * @param auditContext the audit context for exception
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toBucketS3Exception(Exception exception, String resource,
                                                @Nonnull S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    return toBucketS3Exception(exception, resource);
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
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @param auditContext the audit context for exception
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource,
                                                @Nonnull S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    return toObjectS3Exception(exception, resource);
  }

  /**
   * Check if a path in alluxio is a directory.
   *
   * @param fs instance of {@link FileSystem}
   * @param bucketPath bucket complete path
   * @param auditContext the audit context for exception
   */
  public static void checkPathIsAlluxioDirectory(FileSystem fs, String bucketPath,
                                                 @Nullable S3AuditContext auditContext)
      throws S3Exception {
    try {
      URIStatus status = fs.getStatus(new AlluxioURI(bucketPath));
      if (!status.isFolder()) {
        throw new InvalidPathException("Bucket " + bucketPath
            + " is not a valid Alluxio directory.");
      }
    } catch (Exception e) {
      if (auditContext != null) {
        throw toBucketS3Exception(e, bucketPath, auditContext);
      }
      throw toBucketS3Exception(e, bucketPath);
    }
  }

  /**
   * Fetches and returns the corresponding {@link URIStatus} for both
   * the multipart upload temp directory and the Alluxio S3 metadata file.
   * @param metaFs instance of {@link FileSystem} - used for metadata operations
   * @param userFs instance of {@link FileSystem} - under the scope of a user agent
   * @param multipartTempDirUri multipart upload tmp directory URI
   * @param uploadId multipart upload Id
   * @return a list of file statuses:
   *         - first, the multipart upload temp directory
   *         - second, the metadata file
   */
  public static List<URIStatus> checkStatusesForUploadId(
      FileSystem metaFs, FileSystem userFs, AlluxioURI multipartTempDirUri, String uploadId)
      throws AlluxioException, IOException {
    // Verify the multipart upload dir exists and is a folder
    URIStatus multipartTempDirStatus = userFs.getStatus(multipartTempDirUri);
    if (!multipartTempDirStatus.isFolder()) {
      //TODO(czhu): determine intended behavior in this edge-case
      throw new RuntimeException(
          "Alluxio UFS contains a file at intended multipart-upload directory path: "
              + multipartTempDirUri);
    }

    // Verify the multipart upload meta file exists and matches the file ID
    final AlluxioURI metaUri = new AlluxioURI(
        S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId));
    URIStatus metaStatus = metaFs.getStatus(metaUri);
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
    return Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class).toProto();
  }

  /**
   * Checks if authentication is enabled.
   *
   * @param conf Alluxio configuration
   * @return true if authentication is enabled, false otherwise
   */
  public static boolean isAuthenticationEnabled(AlluxioConfiguration conf) {
    return conf.getBoolean(PropertyKey.S3_REST_AUTHENTICATION_ENABLED);
  }

  /**
   * @param user the {@link Subject} name of the filesystem user
   * @param fs the source {@link FileSystem} to base off of
   * @return A {@link FileSystem} with the subject set to the provided user
   */
  public static FileSystem createFileSystemForUser(
      String user, FileSystem fs) {
    if (user == null) {
      // Used to return the top-level FileSystem view when not using Authentication
      return fs;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return FileSystem.Factory.get(subject, fs.getConf());
  }

  /**
   * Convert MultivaluedMap to a single value map.
   *
   * @param queryParameters MultivaluedMap
   * @param lowerCase whether to use lower case
   * @return a single value map
   */
  public static Map<String, String> fromMultiValueToSingleValueMap(
      MultivaluedMap<String, String> queryParameters, boolean lowerCase) {
    Map<String, String> result = lowerCase
        ? new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
              return o1.compareToIgnoreCase(o2);
          }
        }) : new HashMap<>();
    for (String key : queryParameters.keySet()) {
      result.put(key, queryParameters.getFirst(key));
    }
    return result;
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
   * This helper method is used to get the ETag xAttr on an object.
   * @param status The {@link URIStatus} of the object
   * @return the entityTag String, or null if it does not exist
   */
  public static String getEntityTag(URIStatus status) {
    if (status.getXAttr() == null
        || !status.getXAttr().containsKey(S3Constants.ETAG_XATTR_KEY)) {
      return null;
    }
    return new String(status.getXAttr().get(S3Constants.ETAG_XATTR_KEY),
        S3Constants.XATTR_STR_CHARSET);
  }

  /**
   * Get username from header info.
   *
   * @param authorization authorization info
   * @param requestContext request context
   * @return username
   * @throws S3Exception
   */
  public static String getUser(String authorization, ContainerRequestContext requestContext)
      throws S3Exception {
    // TODO(czhu): refactor PropertyKey.S3_REST_AUTHENTICATION_ENABLED to an ENUM
    //             to specify between using custom Authenticator class vs. Alluxio Master schemes
    if (S3RestUtils.isAuthenticationEnabled(Configuration.global())) {
      return getUserFromSignature(requestContext);
    }
    try {
      return getUserFromAuthorization(authorization, Configuration.global());
    } catch (RuntimeException e) {
      throw new S3Exception(new S3ErrorCode(S3ErrorCode.INTERNAL_ERROR.getCode(),
          e.getMessage(), S3ErrorCode.INTERNAL_ERROR.getStatus()));
    }
  }

  /**
   * Get username from parsed header info.
   *
   * @return username
   * @throws S3Exception
   */
  private static String getUserFromSignature(ContainerRequestContext requestContext)
      throws S3Exception {
    AwsSignatureProcessor signatureProcessor = new AwsSignatureProcessor(requestContext);
    Authenticator authenticator = Authenticator.Factory.create(Configuration.global());
    AwsAuthInfo authInfo = signatureProcessor.getAuthInfo();
    if (authenticator.isAuthenticated(authInfo)) {
      return authInfo.getAccessID();
    }
    throw new S3Exception(authInfo.toString(), S3ErrorCode.INVALID_IDENTIFIER);
  }

  /**
   * Gets the user from the authorization header string for AWS Signature Version 4.
   * @param authorization the authorization header string
   * @param conf the {@link AlluxioConfiguration} Alluxio conf
   * @return the user
   */
  @VisibleForTesting
  public static String getUserFromAuthorization(String authorization, AlluxioConfiguration conf)
      throws S3Exception {
    if (conf.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE) == AuthType.NOSASL) {
      return null;
    }
    if (authorization == null) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    // Parse the authorization header defined at
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
    // All other authorization types are deprecated or EOL (as of writing)
    // Example Header value (spaces turned to line breaks):
    // AWS4-HMAC-SHA256
    // Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,
    // SignedHeaders=host;range;x-amz-date,
    // Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024

    // We only care about the credential key, so split the header by " " and then take everything
    // after the "=" and before the first "/"
    String[] fields = authorization.split(" ");
    if (fields.length < 2) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    String credentials = fields[1];
    String[] creds = credentials.split("=");
    // only support version 4 signature
    if (creds.length < 2 || !StringUtils.equals("Credential", creds[0])) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    final String user = creds[1].substring(0, creds[1].indexOf("/")).trim();
    if (user.isEmpty()) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    return user;
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
