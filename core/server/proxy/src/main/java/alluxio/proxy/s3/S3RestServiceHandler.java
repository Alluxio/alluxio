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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.web.ProxyWebServer;

import com.google.common.io.BaseEncoding;
import com.qmino.miredot.annotations.ReturnType;

import org.apache.commons.codec.binary.Hex;

import java.io.InputStream;
import java.security.MessageDigest;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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

  // Bucket is the first component in the URL path.
  public static final String BUCKET_PARAM = "{bucket}/";
  // Object is after bucket in the URL path.
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
   * @param bucket the bucket name
   * @return the response object
   * @summary creates a bucket
   */
  @PUT
  @Path(BUCKET_PARAM)
  @ReturnType("Void")
  public Response createBucket(@PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
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
   * @param bucket the bucket name
   * @return the response object
   * @summary deletes a bucket
   */
  @DELETE
  @Path(BUCKET_PARAM)
  @ReturnType("Void")
  public Response deleteBucket(@PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);

        checkBucketIsAlluxioDirectory(bucketPath);

        // Delete the bucket.
        DeleteOptions options = DeleteOptions.defaults();
        options.setAlluxioOnly(Configuration.get(PropertyKey.PROXY_S3_DELETE_TYPE)
            .equals(Constants.DELETE_IN_ALLUXIO_ONLY));
        try {
          mFileSystem.delete(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.OK;
      }
    });
  }

  /**
   * @summary creates an object without using multipart upload
   * @param contentMD5 the Base64 encoded 128-bit MD5 digest of the object
   * @param bucket the bucket name
   * @param object the object name
   * @param is the request body
   * @return the response object
   */
  @PUT
  @Path(OBJECT_PARAM)
  @ReturnType("Void")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response createObject(@HeaderParam("Content-MD5") final String contentMD5,
                               @PathParam("bucket") final String bucket,
                               @PathParam("object") final String object,
                               final InputStream is) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response>() {
      @Override
      public Response call() throws S3Exception {
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);
        checkBucketIsAlluxioDirectory(bucketPath);
        AlluxioURI objectURI = new AlluxioURI(bucketPath + AlluxioURI.SEPARATOR + object);

        try {
          CreateFileOptions options = CreateFileOptions.defaults().setRecursive(true)
              .setWriteType(getS3WriteType());
          FileOutStream os = mFileSystem.createFile(objectURI, options);
          MessageDigest md5 = MessageDigest.getInstance("MD5");

          byte[] buf = new byte[4096];
          while (true) {
            int len = is.read(buf);
            if (len == -1) {
              break;
            }
            md5.update(buf, 0, len);
            os.write(buf, 0, len);
          }
          os.close();

          byte[] digest = md5.digest();
          String base64Digest = BaseEncoding.base64().encode(digest);
          if (!contentMD5.equals(base64Digest)) {
            // The object may be corrupted, delete the written object and return an error.
            mFileSystem.delete(objectURI);
            throw new S3Exception(objectURI.getPath(), S3ErrorCode.BAD_DIGEST);
          }

          String entityTag = Hex.encodeHexString(digest);
          return Response.ok().tag(entityTag).build();
        } catch (Exception e) {
          throw toObjectS3Exception(e, bucketPath);
        }
      }
    });
  }

  /**
   * @param bucket the bucket name
   * @param object the object name
   * @return the response object
   * @summary deletes an object
   */
  @DELETE
  @Path(OBJECT_PARAM)
  @ReturnType("Void")
  public Response deleteObject(@PathParam("bucket") final String bucket,
                               @PathParam("object") final String object) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Response.Status>() {
      @Override
      public Response.Status call() throws S3Exception {
        String bucketPath = parseBucketPath(AlluxioURI.SEPARATOR + bucket);

        // Delete the object.
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
        DeleteOptions options = DeleteOptions.defaults();
        options.setAlluxioOnly(Configuration.get(PropertyKey.PROXY_S3_DELETE_TYPE)
            .equals(Constants.DELETE_IN_ALLUXIO_ONLY));
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

  private WriteType getS3WriteType() {
    return Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class);
  }
}
