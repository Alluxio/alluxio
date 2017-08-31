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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.web.ProxyWebServer;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.qmino.miredot.annotations.ReturnType;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;
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
@Consumes(MediaType.APPLICATION_XML)
public final class S3RestServiceHandler {
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

  /**
   * Constructs a new {@link S3RestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public S3RestServiceHandler(@Context ServletContext context) {
    mFileSystem =
        (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary creates a bucket
   * @param bucket the bucket name
   * @return the response object
   */
  @PUT
  @Path(BUCKET_PARAM)
  @ReturnType("java.lang.Void")
  public Response createBucket(@PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);

        // Create the bucket.
        CreateDirectoryOptions options = CreateDirectoryOptions.defaults()
            .setWriteType(getS3WriteType());
        try {
          mFileSystem.createDirectory(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.OK;
      }
    });
  }

  /**
   * @summary deletes a bucket
   * @param bucket the bucket name
   * @return the response object
   */
  @DELETE
  @Path(BUCKET_PARAM)
  @ReturnType("java.lang.Void")
  public Response deleteBucket(@PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);

        checkBucketIsAlluxioDirectory(bucketPath);

        // Delete the bucket.
        DeleteOptions options = DeleteOptions.defaults();
        options.setAlluxioOnly(Configuration.get(PropertyKey.PROXY_S3_DELETE_TYPE)
            .equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY));
        try {
          mFileSystem.delete(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.NO_CONTENT;
      }
    });
  }

  /**
   * @summary gets a bucket and lists all the objects in it
   * @param bucket the bucket name
   * @param continuationToken the optional continuation token param
   * @param maxKeys the optional max keys param
   * @param prefix the optional prefix param
   * @return the response object
   */
  @GET
  @Path(BUCKET_PARAM)
  @ReturnType("alluxio.proxy.s3.ListBucketResult")
  // TODO(chaomin): consider supporting more request params like prefix and delimiter.
  public Response getBucket(@PathParam("bucket") final String bucket,
      @QueryParam("continuation-token") final String continuationToken,
      @QueryParam("max-keys") final String maxKeys,
      @QueryParam("prefix") final String prefix) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<ListBucketResult>() {
      @Override
      public ListBucketResult call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);

        checkBucketIsAlluxioDirectory(bucketPath);

        List<URIStatus> objects;
        ListBucketOptions listBucketOptions = ListBucketOptions.defaults()
            .setContinuationToken(continuationToken)
            .setMaxKeys(maxKeys)
            .setPrefix(prefix);
        try {
          objects = listObjects(new AlluxioURI(bucketPath), listBucketOptions);
          ListBucketResult response = new ListBucketResult(bucketPath, objects, listBucketOptions);
          return response;
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
      }
    });
  }

  /**
   * @summary uploads an object
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
  @ReturnType("java.lang.Void")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response createObject(@HeaderParam("Content-MD5") final String contentMD5,
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

        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        checkBucketIsAlluxioDirectory(bucketPath);

        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        if (partNumber != null) {
          // This object is part of a multipart upload, should be uploaded into the temporary
          // directory first.
          String tmpDir = S3RestUtils.getMultipartTemporaryDirForObject(bucket, object);
          long tmpDirFileId = -1;
          try {
            tmpDirFileId = mFileSystem.getStatus(new AlluxioURI(tmpDir)).getFileId();
          } catch (Exception e) {
            throw toObjectS3Exception(e, tmpDir);
          }
          if (uploadId != tmpDirFileId) {
            throw new S3Exception(objectPath, S3ErrorCode.NO_SUCH_UPLOAD);
          }
          objectPath = tmpDir + AlluxioURI.SEPARATOR + Integer.toString(partNumber);
        }
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          CreateFileOptions options = CreateFileOptions.defaults().setRecursive(true)
              .setWriteType(getS3WriteType());
          FileOutStream os = mFileSystem.createFile(objectURI, options);
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
              mFileSystem.delete(objectURI);
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
   * @summary initiates a multipart upload
   * @param bucket the bucket name
   * @param object the object name
   * @param uploads the query parameter specifying that this request is to initiate a multipart
   *                upload instead of uploading an object through HTTP multipart forms
   * @return the response object
   */
  @POST
  @Path(OBJECT_PARAM)
  @ReturnType("alluxio.proxy.s3.InitiateMultipartUploadResult")
  public Response initiateMultipartUpload(@PathParam("bucket") final String bucket,
      @PathParam("object") final String object, @QueryParam("uploads") final String uploads) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<InitiateMultipartUploadResult>() {
      @Override
      public InitiateMultipartUploadResult call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        Preconditions.checkNotNull(uploads, "required 'uploads' parameter is missing");

        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        checkBucketIsAlluxioDirectory(bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI multipartTemporaryDir =
            new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucket, object));

        try {
          mFileSystem.createDirectory(multipartTemporaryDir);
          long uploadId = mFileSystem.getStatus(multipartTemporaryDir).getFileId();
          return new InitiateMultipartUploadResult(bucket, object, Long.toString(uploadId));
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary retrieves an object's metadata
   * @param bucket the bucket name
   * @param object the object name
   * @return the response object
   */
  @HEAD
  @Path(OBJECT_PARAM)
  @ReturnType("java.lang.Void")
  public Response getObjectMetadata(@PathParam("bucket") final String bucket,
      @PathParam("object") final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");

        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        checkBucketIsAlluxioDirectory(bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          URIStatus status = mFileSystem.getStatus(objectURI);
          // TODO(cc): Consider how to respond with the object's ETag.
          return Response.ok()
              .lastModified(new Date(status.getLastModificationTimeMs()))
              .header(S3Constants.S3_CONTENT_LENGTH_HEADER, status.getLength())
              .build();
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * @summary downloads an object
   * @param bucket the bucket name
   * @param object the object name
   * @return the response object
   */
  @GET
  @Path(OBJECT_PARAM)
  @ReturnType("java.io.InputStream")
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_OCTET_STREAM})
  public Response getObject(@PathParam("bucket") final String bucket,
      @PathParam("object") final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");

        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        checkBucketIsAlluxioDirectory(bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        AlluxioURI objectURI = new AlluxioURI(objectPath);

        try {
          URIStatus status = mFileSystem.getStatus(objectURI);
          FileInStream is = mFileSystem.openFile(objectURI);
          // TODO(cc): Consider how to respond with the object's ETag.
          return Response.ok(is)
              .lastModified(new Date(status.getLastModificationTimeMs()))
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
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId the upload ID which identifies the incomplete multipart upload to be aborted
   * @return the response object
   */
  @DELETE
  @Path(OBJECT_PARAM)
  @ReturnType("java.lang.Void")
  public Response deleteObject(@PathParam("bucket") final String bucket,
      @PathParam("object") final String object, @QueryParam("uploadId") final Long uploadId) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");

        if (uploadId != null) {
          // Abort an incomplete multipart upload.
          abortMultipartUpload(bucket, object, uploadId);
          // Note: the normal response for S3 abort multipart upload is 204 NO_CONTENT, not 200 OK
          return Response.Status.NO_CONTENT;
        }

        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        // Delete the object.
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        DeleteOptions options = DeleteOptions.defaults();
        options.setAlluxioOnly(Configuration.get(PropertyKey.PROXY_S3_DELETE_TYPE)
            .equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY));
        try {
          mFileSystem.delete(new AlluxioURI(objectPath), options);
        } catch (Exception e) {
          throw toObjectS3Exception(e, objectPath);
        }
        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return Response.Status.NO_CONTENT;
      }
    });
  }

  private S3Exception toBucketS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(resource, S3ErrorCode.BUCKET_NOT_EMPTY);
    } catch (FileAlreadyExistsException e) {
      return new S3Exception(resource, S3ErrorCode.BUCKET_ALREADY_EXISTS);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(resource, S3ErrorCode.NO_SUCH_BUCKET);
    } catch (InvalidPathException e) {
      return new S3Exception(resource, S3ErrorCode.INVALID_BUCKET_NAME);
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
      return new S3Exception(resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  private String parseBucketPath(String bucketPath) throws S3Exception {
    if (!bucketPath.contains(BUCKET_SEPARATOR)) {
      return bucketPath;
    }
    String normalizedPath = bucketPath.replace(BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
    checkNestedBucketIsUnderMountPoint(normalizedPath);
    return normalizedPath;
  }

  private void checkNestedBucketIsUnderMountPoint(String bucketPath) throws S3Exception {
    // Assure that the bucket is directly under a mount point.
    AlluxioURI parent = new AlluxioURI(bucketPath).getParent();
    try {
      if (!mFileSystem.getMountTable().containsKey(parent.getPath())) {
        throw new S3Exception(bucketPath, S3ErrorCode.INVALID_NESTED_BUCKET_NAME);
      }
    } catch (Exception e) {
      throw toBucketS3Exception(e, bucketPath);
    }
  }

  private void checkBucketIsAlluxioDirectory(String bucketPath) throws S3Exception {
    try {
      URIStatus status = mFileSystem.getStatus(new AlluxioURI(bucketPath));
      if (!status.isFolder()) {
        throw new InvalidPathException("Bucket name is not a valid Alluxio directory.");
      }
    } catch (Exception e) {
      throw toBucketS3Exception(e, bucketPath);
    }
  }

  private List<URIStatus> listObjects(AlluxioURI uri, ListBucketOptions listBucketOptions)
      throws FileDoesNotExistException, IOException, AlluxioException {
    List<URIStatus> objects = new ArrayList<>();
    Queue<URIStatus> traverseQueue = new ArrayDeque<>();

    List<URIStatus> children;
    String prefix = listBucketOptions.getPrefix();
    if (prefix != null && prefix.contains(AlluxioURI.SEPARATOR)) {
      AlluxioURI prefixDirUri = new AlluxioURI(uri.getPath() + AlluxioURI.SEPARATOR
          + prefix.substring(0, prefix.lastIndexOf(AlluxioURI.SEPARATOR)));
      children = mFileSystem.listStatus(prefixDirUri);
    } else {
      children = mFileSystem.listStatus(uri);
    }
    traverseQueue.addAll(children);
    while (!traverseQueue.isEmpty()) {
      URIStatus cur = traverseQueue.remove();
      if (!cur.isFolder()) {
        // Alluxio file is an object.
        objects.add(cur);
      } else {
        List<URIStatus> curChildren = mFileSystem.listStatus(new AlluxioURI(cur.getPath()));
        if (curChildren.isEmpty()) {
          // An empty Alluxio directory is considered as a valid object.
          objects.add(cur);
        } else {
          traverseQueue.addAll(curChildren);
        }
      }
    }
    return objects;
  }

  private WriteType getS3WriteType() {
    return Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class);
  }

  private void abortMultipartUpload(String bucket, String object, long uploadId)
      throws S3Exception {
      String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
      checkBucketIsAlluxioDirectory(bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
      AlluxioURI multipartTemporaryDir =
          new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucket, object));

      try {
        if (!mFileSystem.exists(multipartTemporaryDir)) {
          throw new S3Exception(multipartTemporaryDir.getPath(), S3ErrorCode.NO_SUCH_UPLOAD);
        }
        long tmpDirId = mFileSystem.getStatus(multipartTemporaryDir).getFileId();
        if (uploadId != tmpDirId) {
          throw new S3Exception(multipartTemporaryDir.getPath(), S3ErrorCode.NO_SUCH_UPLOAD);
        }
        mFileSystem.delete(multipartTemporaryDir, DeleteOptions.defaults().setRecursive(true));
      } catch (Exception e) {
        throw toObjectS3Exception(e, objectPath);
      }
  }
}
