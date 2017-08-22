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
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.web.ProxyWebServer;

import com.qmino.miredot.annotations.ReturnType;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
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
   * @param bucket the Alluxio path
   * @return the response object
   */
  @PUT
  @Path(BUCKET_PARAM)
  @ReturnType("java.lang.Void")
  public Response createBucket(@PathParam("bucket") final String bucket) {
    return S3RestUtils.call(bucket, new S3RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws S3Exception {
        String bucketPath =  AlluxioURI.SEPARATOR + bucket;
        if (bucketPath.contains(BUCKET_SEPARATOR)) {
          bucketPath = bucketPath.replace(BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
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

        // Create the bucket.
        CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
        options.setWriteType(Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE,
            WriteType.class));
        try {
          mFileSystem.createDirectory(new AlluxioURI(bucketPath), options);
        } catch (Exception e) {
          throw toBucketS3Exception(e, bucketPath);
        }
        return null;
      }
    });
  }

  private S3Exception toBucketS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (InvalidPathException e) {
      return new S3Exception(resource, S3ErrorCode.INVALID_BUCKET_NAME);
    } catch (FileAlreadyExistsException e) {
      return new S3Exception(resource, S3ErrorCode.BUCKET_ALREADY_EXISTS);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }
}
