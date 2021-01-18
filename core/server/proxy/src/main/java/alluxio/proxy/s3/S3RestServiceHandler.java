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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.User;
import alluxio.web.ProxyWebServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for Amazon S3 API.
 */
@NotThreadSafe
@Path(S3RestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_XML)
@Consumes({ MediaType.TEXT_XML, MediaType.APPLICATION_XML })
public final class S3RestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestServiceHandler.class);

  public static final String SERVICE_PREFIX = "s3";

  /**
   * Bucket must be a directory directly under a mount point. If it is under a non-root mount point,
   * the bucket separator must be used as the separator in the bucket name, for example,
   * mount:point:bucket represents Alluxio directory /mount/point/bucket.
   */
  public static final String BUCKET_SEPARATOR = ":";

  /* Bucket is the first component in the URL path. */
  public static final String BUCKET_PARAM = "{bucket}/";
  /* Object is after bucket in the URL path */
  public static final String OBJECT_PARAM = "{bucket}/{object:.+}";

  private final FileSystem mFileSystem;
  private final InstancedConfiguration mSConf;

  /**
   * Constructs a new {@link S3RestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public S3RestServiceHandler(@Context ServletContext context) {
    mFileSystem =
        (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
    mSConf = (InstancedConfiguration)
        context.getAttribute(ProxyWebServer.SERVER_CONFIGURATION_RESOURCE_KEY);
  }

  /**
   * Gets the user from the authorization header string.
   * @param authorization the authorization header string
   * @return the user
   */
  @VisibleForTesting
  public static String getUserFromAuthorization(String authorization) {
    if (authorization == null) {
      return null;
    }

    // The valid pattern for Authorization is "AWS <AWSAccessKeyId>:<Signature>"
    int spaceIndex = authorization.indexOf(' ');
    if (spaceIndex == -1) {
      return null;
    }
    String stripped = authorization.substring(spaceIndex + 1);

    int colonIndex = stripped.indexOf(':');
    if (colonIndex == -1) {
      return null;
    }

    final String user = stripped.substring(0, colonIndex);
    if (user.isEmpty()) {
      return null;
    }

    return user;
  }

  private FileSystem getFileSystem(String authorization) {
    final String user = getUserFromAuthorization(authorization);

    if (user == null) {
      return mFileSystem;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return FileSystem.Factory.get(subject, mSConf);
  }

  /**
   * @summary lists all buckets owned by you
   * @param authorization header parameter authorization
   * @return the response object
   */
  @GET
  @Path("/")
  public Response listAllMyBuckets(@HeaderParam("Authorization") String authorization) {

    return S3RestUtils.call("", new S3RestUtils.RestCallable<ListAllMyBucketsResult>() {
      @Override
      public ListAllMyBucketsResult call() throws S3Exception {
        String user = getUserFromAuthorization(authorization);

        List<URIStatus> objects;
        try {
          objects = getFileSystem(authorization).listStatus(new AlluxioURI("/"));
        } catch (AlluxioException | IOException e) {
          throw new RuntimeException(e);
        }

        final List<String> buckets = objects.stream()
            .filter((uri) -> uri.getOwner().equals(user))
            .map((uri) -> uri.getName()).collect(Collectors.toList());
        ListAllMyBucketsResult response = new ListAllMyBucketsResult(buckets);
        return response;
      }
    });
  }

  /**
   * @summary gets a bucket and lists all the objects in it
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param markerParam the optional marker param
   * @param prefixParam the optional prefix param
   * @param delimiterParam the optional delimiter param
   * @param maxKeysParam the optional max keys param
   * @return the response object
   */
  @GET
  @Path(BUCKET_PARAM)
  //@ReturnType("alluxio.proxy.s3.ListBucketResult")
  public Response getBucket(@HeaderParam("Authorization") String authorization,
                            @PathParam("bucket") final String bucket,
                            @QueryParam("marker") final String markerParam,
                            @QueryParam("prefix") final String prefixParam,
                            @QueryParam("delimiter") final String delimiterParam,
                            @QueryParam("max-keys") final int maxKeysParam) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<ListBucketResult>() {
      @Override
      public ListBucketResult call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");

        String marker = markerParam;
        if (marker == null) {
          marker = "";
        }

        String prefix = prefixParam;
        if (prefix == null) {
          prefix = "";
        }

        String delimiter = delimiterParam;
        if (delimiter == null) {
          delimiter = AlluxioURI.SEPARATOR;
        }

        int maxKeys = maxKeysParam;
        if (maxKeys <= 0) {
          maxKeys = ListBucketOptions.DEFAULT_MAX_KEYS;
        }

        String path = parsePath(AlluxioURI.SEPARATOR + bucket, prefix, delimiter);

        final FileSystem fs = getFileSystem(authorization);
        checkPathIsAlluxioDirectory(fs, path);

        List<URIStatus> children;
        ListBucketOptions listBucketOptions = ListBucketOptions.defaults()
            .setMarker(marker)
            .setPrefix(prefix)
            .setMaxKeys(maxKeys);
        try {
          children = fs.listStatus(new AlluxioURI(path));
        } catch (IOException | AlluxioException e) {
          throw new RuntimeException(e);
        }
        ListBucketResult response = new ListBucketResult(
            bucket,
            children,
            listBucketOptions);
        return response;
      }
    });
  }

  /**
   * @summary creates a bucket
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @return the response object
   */
  @PUT
  @Path(BUCKET_PARAM)
  //@ReturnType("java.lang.Void")
  public Response createBucket(@HeaderParam("Authorization") String authorization,
                               @PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");

        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);

        final FileSystem fs = getFileSystem(authorization);
        // Create the bucket.
        CreateDirectoryPOptions options =
            CreateDirectoryPOptions.newBuilder().setWriteType(getS3WriteType()).build();
        try {
          fs.createDirectory(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.OK;
      }
    });
  }

  /**
   * @summary deletes a bucket
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @return the response object
   */
  @DELETE
  @Path(BUCKET_PARAM)
  //@ReturnType("java.lang.Void")
  public Response deleteBucket(@HeaderParam("Authorization") String authorization,
                               @PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);

        final FileSystem fs = getFileSystem(authorization);
        checkPathIsAlluxioDirectory(fs, bucketPath);

        // Delete the bucket.
        DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(ServerConfiguration
            .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
            .build();
        try {
          fs.delete(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.NO_CONTENT;
      }
    });
  }

  /**
   * @summary uploads an object or part of an object in multipart upload
   * @param authorization header parameter authorization
   * @param contentMD5 the optional Base64 encoded 128-bit MD5 digest of the object
   * @param bucket the bucket name
   * @param object the object name
   * @param partNumber the identification of the part of the object in multipart upload,
   *                   otherwise null
   * @param uploadId the upload ID of the multipart upload, otherwise null
   * @param is the request body
   * @return the response object
   */
  @PUT
  @Path(OBJECT_PARAM)
  //@ReturnType("java.lang.Void")
  @Consumes(MediaType.WILDCARD)
  public Response createObjectOrUploadPart(@HeaderParam("Authorization") String authorization,
                                           @HeaderParam("Content-MD5") final String contentMD5,
                                           @PathParam("bucket") final String bucket,
                                           @PathParam("object") final String object,
                                           @QueryParam("partNumber") final Integer partNumber,
                                           @QueryParam("uploadId") final Long uploadId,
                                           final InputStream is) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        Preconditions.checkArgument((partNumber == null && uploadId == null)
            || (partNumber != null && uploadId != null),
            "'partNumber' and 'uploadId' parameter should appear together or be "
            + "missing together.");

        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);

        final FileSystem fs = getFileSystem(authorization);
        checkPathIsAlluxioDirectory(fs, bucketPath);

        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

        if (objectPath.endsWith(AlluxioURI.SEPARATOR)) {
          // Need to create a folder
          try {
            fs.createDirectory(new AlluxioURI(objectPath));
          } catch (IOException | AlluxioException e) {
            throw toObjectS3Exception(e, objectPath);
          }
          return Response.ok().build();
        }

        if (partNumber != null) {
          // This object is part of a multipart upload, should be uploaded into the temporary
          // directory first.
          String tmpDir = S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object);
          checkUploadId(fs, new AlluxioURI(tmpDir), uploadId);
          objectPath = tmpDir + AlluxioURI.SEPARATOR + Integer.toString(partNumber);
        }
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          CreateFilePOptions options = CreateFilePOptions.newBuilder().setRecursive(true)
              .setWriteType(getS3WriteType()).build();
          FileOutStream os = fs.createFile(objectURI, options);
          MessageDigest md5 = MessageDigest.getInstance("MD5");
          DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5);

          try {
            ByteStreams.copy(is, digestOutputStream);
          } finally {
            digestOutputStream.close();
          }

          byte[] digest = md5.digest();
          String base64Digest = BaseEncoding.base64().encode(digest);
          if (contentMD5 != null && !contentMD5.equals(base64Digest)) {
            // The object may be corrupted, delete the written object and return an error.
            try {
              fs.delete(objectURI);
            } catch (Exception e2) {
              // intend to continue and return BAD_DIGEST S3Exception.
            }
            throw new S3Exception(objectURI.getPath(), S3ErrorCode.BAD_DIGEST);
          }

          String entityTag = Hex.encodeHexString(digest);
          return Response.ok().tag(entityTag).build();
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary initiates or completes a multipart upload based on query parameters
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @param uploads the query parameter specifying that this request is to initiate a multipart
   *                upload instead of uploading an object through HTTP multipart forms
   * @param uploadId the ID of the multipart upload to be completed
   * @return the response object
   */
  @POST
  @Path(OBJECT_PARAM)
  // TODO(cc): investigate on how to specify multiple return types, and how to decouple the REST
  // endpoints where the only difference is the query parameter.
  public Response initiateOrCompleteMultipartUpload(
      @HeaderParam("Authorization") String authorization,
      @PathParam("bucket") final String bucket,
      @PathParam("object") final String object,
      @QueryParam("uploads") final String uploads,
      @QueryParam("uploadId") final Long uploadId) {
    Preconditions.checkArgument(uploads != null || uploadId != null,
        "parameter 'uploads' or 'uploadId' should exist");
    final FileSystem fileSystem = getFileSystem(authorization);
    if (uploads != null) {
      return initiateMultipartUpload(fileSystem, bucket, object);
    } else {
      return completeMultipartUpload(fileSystem, bucket, object, uploadId);
    }
  }

  private Response initiateMultipartUpload(final FileSystem fs,
                                           final String bucket,
                                           final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<InitiateMultipartUploadResult>() {
      @Override
      public InitiateMultipartUploadResult call() throws S3Exception {
        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
        checkPathIsAlluxioDirectory(fs, bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI multipartTemporaryDir =
            new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object));

        try {
          fs.createDirectory(multipartTemporaryDir);
          // Use the file ID of multipartTemporaryDir as the upload ID.
          long uploadId = fs.getStatus(multipartTemporaryDir).getFileId();
          return new InitiateMultipartUploadResult(bucket, object, Long.toString(uploadId));
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  // TODO(cc): support the options in the XML request body defined in
  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html, currently, the parts
  // under the temporary multipart upload directory are combined into the final object.
  private Response completeMultipartUpload(final FileSystem fs,
                                           final String bucket,
                                           final String object,
                                           final long uploadId) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<CompleteMultipartUploadResult>() {
      @Override
      public CompleteMultipartUploadResult call() throws S3Exception {
        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
        checkPathIsAlluxioDirectory(fs, bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI multipartTemporaryDir =
            new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object));
        checkUploadId(fs, multipartTemporaryDir, uploadId);

        try {
          List<URIStatus> parts = fs.listStatus(multipartTemporaryDir);
          Collections.sort(parts, new URIStatusNameComparator());

          CreateFilePOptions options = CreateFilePOptions.newBuilder().setRecursive(true)
              .setWriteType(getS3WriteType()).build();
          FileOutStream os = fs.createFile(new AlluxioURI(objectPath), options);
          MessageDigest md5 = MessageDigest.getInstance("MD5");
          DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5);

          try {
            for (URIStatus part : parts) {
              try (FileInStream is = fs.openFile(new AlluxioURI(part.getPath()))) {
                ByteStreams.copy(is, digestOutputStream);
              }
            }
          } finally {
            digestOutputStream.close();
          }

          fs.delete(multipartTemporaryDir,
              DeletePOptions.newBuilder().setRecursive(true).build());

          String entityTag = Hex.encodeHexString(md5.digest());
          return new CompleteMultipartUploadResult(objectPath, bucket, object, entityTag);
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary retrieves an object's metadata
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @return the response object
   */
  @HEAD
  @Path(OBJECT_PARAM)
  //@ReturnType("java.lang.Void")
  public Response getObjectMetadata(@HeaderParam("Authorization") String authorization,
                                    @PathParam("bucket") final String bucket,
                                    @PathParam("object") final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");

        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);

        final FileSystem fs = getFileSystem(authorization);
        checkPathIsAlluxioDirectory(fs, bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          URIStatus status = fs.getStatus(objectURI);
          // TODO(cc): Consider how to respond with the object's ETag.
          return Response.ok()
              .lastModified(new Date(status.getLastModificationTimeMs()))
              .header(S3Constants.S3_ETAG_HEADER, "\"" + status.getLastModificationTimeMs() + "\"")
              .header(S3Constants.S3_CONTENT_LENGTH_HEADER, status.getLength())
              .build();
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary downloads an object or list parts of the object in multipart upload
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId the ID of the multipart upload, if not null, listing parts of the object
   * @return the response object
   */
  @GET
  @Path(OBJECT_PARAM)
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_OCTET_STREAM})
  public Response getObjectOrListParts(@HeaderParam("Authorization") String authorization,
                                       @PathParam("bucket") final String bucket,
                                       @PathParam("object") final String object,
                                       @QueryParam("uploadId") final Long uploadId) {
    Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
    Preconditions.checkNotNull(object, "required 'object' parameter is missing");

    final FileSystem fs = getFileSystem(authorization);

    if (uploadId != null) {
      return listParts(fs, bucket, object, uploadId);
    } else {
      return getObject(fs, bucket, object);
    }
  }

  // TODO(cc): support paging during listing parts, currently, all parts are returned at once.
  private Response listParts(final FileSystem fs,
                             final String bucket,
                             final String object,
                             final long uploadId) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<ListPartsResult>() {
      @Override
      public ListPartsResult call() throws S3Exception {
        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
        checkPathIsAlluxioDirectory(fs, bucketPath);

        AlluxioURI tmpDir = new AlluxioURI(
            S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object));
        checkUploadId(fs, tmpDir, uploadId);

        try {
          List<URIStatus> statuses = fs.listStatus(tmpDir);
          Collections.sort(statuses, new URIStatusNameComparator());

          List<ListPartsResult.Part> parts = new ArrayList<>();
          for (URIStatus status : statuses) {
            parts.add(ListPartsResult.Part.fromURIStatus(status));
          }

          ListPartsResult result = new ListPartsResult();
          result.setBucket(bucketPath);
          result.setKey(object);
          result.setUploadId(Long.toString(uploadId));
          result.setParts(parts);
          return result;
        } catch (Exception e) {
          throw toObjectS3Exception(e, tmpDir.getPath());
        }
      }
    });
  }

  private Response getObject(final FileSystem fs,
                             final String bucket,
                             final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {

        String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
        checkPathIsAlluxioDirectory(fs, bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          URIStatus status = fs.getStatus(objectURI);
          FileInStream is = fs.openFile(objectURI);
          // TODO(cc): Consider how to respond with the object's ETag.
          return Response.ok(is)
              .lastModified(new Date(status.getLastModificationTimeMs()))
              .header(S3Constants.S3_ETAG_HEADER, "\"" + status.getLastModificationTimeMs() + "\"")
              .header(S3Constants.S3_CONTENT_LENGTH_HEADER, status.getLength())
              .build();
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary deletes a object
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId the upload ID which identifies the incomplete multipart upload to be aborted
   * @return the response object
   */
  @DELETE
  @Path(OBJECT_PARAM)
  //@ReturnType("java.lang.Void")
  public Response deleteObjectOrAbortMultipartUpload(
      @HeaderParam("Authorization") String authorization,
      @PathParam("bucket") final String bucket,
      @PathParam("object") final String object,
      @QueryParam("uploadId") final Long uploadId) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");

        final FileSystem fs = getFileSystem(authorization);

        if (uploadId != null) {
          abortMultipartUpload(fs, bucket, object, uploadId);
        } else {
          deleteObject(fs, bucket, object);
        }

        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return Response.Status.NO_CONTENT;
      }
    });
  }

  // TODO(cc): Support automatic abortion after a timeout.
  private void abortMultipartUpload(FileSystem fs, String bucket, String object, long uploadId)
      throws S3Exception {
    String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
    checkPathIsAlluxioDirectory(fs, bucketPath);
    String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    AlluxioURI multipartTemporaryDir =
        new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object));
    checkUploadId(fs, multipartTemporaryDir, uploadId);

    try {
      fs.delete(multipartTemporaryDir,
          DeletePOptions.newBuilder().setRecursive(true).build());
    } catch (Exception e) {
      throw toObjectS3Exception(e, objectPath);
    }
  }

  private void deleteObject(FileSystem fs, String bucket, String object) throws S3Exception {
    String bucketPath = parsePath(AlluxioURI.SEPARATOR + bucket);
    // Delete the object.
    String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(ServerConfiguration
        .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY)).build();
    try {
      fs.delete(new AlluxioURI(objectPath), options);
    } catch (Exception e) {
      throw toObjectS3Exception(e, objectPath);
    }
  }

  private S3Exception toBucketS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
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

  private S3Exception toObjectS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  private String parsePath(String bucketPath) throws S3Exception {
    return parsePath(bucketPath, null, null);
  }

  private String parsePath(String bucketPath, String prefix, String delimiter) throws S3Exception {
    if (prefix == null) {
      prefix = "";
    }

    if (delimiter == null) {
      delimiter = AlluxioURI.SEPARATOR;
    }

    String normalizedBucket = bucketPath.replace(BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
    String normalizedPrefix = prefix.replace(delimiter, AlluxioURI.SEPARATOR);

    if (!normalizedPrefix.isEmpty()) {
      normalizedPrefix = AlluxioURI.SEPARATOR + normalizedPrefix;
    }
    return normalizedBucket + normalizedPrefix;
  }

  private void checkPathIsAlluxioDirectory(FileSystem fs, String bucketPath) throws S3Exception {
    try {
      URIStatus status = fs.getStatus(new AlluxioURI(bucketPath));
      if (!status.isFolder()) {
        throw new InvalidPathException("Bucket name is not a valid Alluxio directory.");
      }
    } catch (Exception e) {
      throw toBucketS3Exception(e, bucketPath);
    }
  }

  private void checkUploadId(FileSystem fs, AlluxioURI multipartTemporaryDir, long uploadId)
      throws S3Exception {
    try {
      if (!fs.exists(multipartTemporaryDir)) {
        throw new S3Exception(multipartTemporaryDir.getPath(), S3ErrorCode.NO_SUCH_UPLOAD);
      }
      long tmpDirId = fs.getStatus(multipartTemporaryDir).getFileId();
      if (uploadId != tmpDirId) {
        throw new S3Exception(multipartTemporaryDir.getPath(), S3ErrorCode.NO_SUCH_UPLOAD);
      }
    } catch (Exception e) {
      throw toObjectS3Exception(e, multipartTemporaryDir.getPath());
    }
  }

  private WritePType getS3WriteType() {
    return ServerConfiguration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class).toProto();
  }

  private class URIStatusNameComparator implements Comparator<URIStatus> {
    @Override
    public int compare(URIStatus o1, URIStatus o2) {
      long part1 = Long.parseLong(o1.getName());
      long part2 = Long.parseLong(o2.getName());
      return Long.compare(part1, part2);
    }
  }
}
