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

package alluxio.worker.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.wire.FileInfo;
import alluxio.worker.dora.DoraWorker;
import java.util.Date;
import com.google.common.base.Preconditions;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3NettyObjectTask extends S3NettyBaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyObjectTask.class);

  protected S3NettyObjectTask(S3NettyHandler handler, OpType opType) {
    super(handler, opType);
  }

  @Override
  public HttpResponse continueTask() {
    return NettyRestUtils.call(mHandler.getBucket(), () -> {
      throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED);
    });
  }

  /**
   * Concatenate bucket and object to make a full path.
   * @return full path
   */
  public String getObjectTaskResource() {
    return mHandler.getBucket() + AlluxioURI.SEPARATOR + mHandler.getObject();
  }

  /**
   * Factory for getting a S3ObjectTask.
   */
  public static final class Factory {
    /**
     * Marshall the request and create corresponding object level S3 task.
     * @param handler
     * @return S3ObjectTask
     */
    public static S3NettyObjectTask create(S3NettyHandler handler) {
      switch (handler.getHttpMethod()) {
        case "GET":
          return new HeadObjectTask(handler, OpType.HeadObject);
//          if (handler.getQueryParameter("uploadId") != null) {
//            return new ListPartsTask(handler, OpType.ListParts);
//          } else if (handler.getQueryParameter("tagging") != null) {
//            return new GetObjectTaggingTask(handler, OpType.GetObjectTagging);
//          } else {
//            return new GetObjectTask(handler, OpType.GetObject);
//          }
//        case "PUT":
//          if (handler.getQueryParameter("tagging") != null) {
//            return new PutObjectTaggingTask(handler, OpType.PutObjectTagging);
//          } else if (handler.getQueryParameter("uploadId") != null) {
//            if (handler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
//              return new UploadPartTask(handler, OpType.UploadPartCopy);
//            }
//            return new UploadPartTask(handler, OpType.UploadPart);
//          } else {
//            if (handler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
//              return new CopyObjectTask(handler, OpType.CopyObject);
//            }
//            return new PutObjectTask(handler, OpType.PutObject);
//          }
        case "POST":
//          if (handler.getQueryParameter("uploads") != null) {
//            return new CreateMultipartUploadTask(handler, OpType.CreateMultipartUpload);
//          } else if (handler.getQueryParameter("uploadId") != null) {
//            return new CompleteMultipartUploadTask(handler, OpType.CompleteMultipartUpload);
//          }
          break;
        case "HEAD":
          return new HeadObjectTask(handler, OpType.HeadObject);
//        case "DELETE":
//          if (handler.getQueryParameter("uploadId") != null) {
//            return new AbortMultipartUploadTask(handler, OpType.AbortMultipartUpload);
//          } else if (handler.getQueryParameter("tagging") != null) {
//            return new DeleteObjectTaggingTask(handler, OpType.DeleteObjectTagging);
//          } else {
//            return new DeleteObjectTask(handler, OpType.DeleteObject);
//          }
        default:
          return new S3NettyObjectTask(handler, OpType.Unsupported);
      }
      return new S3NettyObjectTask(handler, OpType.Unsupported);
    }
  }

  private static final class HeadObjectTask extends S3NettyObjectTask {

    public HeadObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(getObjectTaskResource(), () -> {
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        String bucketPath = AlluxioURI.SEPARATOR + mHandler.getBucket();
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
//            if (userFs instanceof DoraCacheFileSystem) {
            AlluxioURI ufsFullPath =
                ((DoraCacheFileSystem) userFs).convertAlluxioPathToUFSPath(objectUri);
            DoraWorker doraWorker = mHandler.getDoraWorker();
            FileInfo fi = doraWorker.getFileInfo(ufsFullPath.toString(), GetStatusPOptions.getDefaultInstance());
//            }
//            URIStatus status = userFs.getStatus(objectUri);
            if (fi.isFolder() && !mHandler.getObject().endsWith(AlluxioURI.SEPARATOR)) {
              throw new FileDoesNotExistException(fi.getPath() + " is a directory");
            }
            HttpResponse response =
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(HttpHeaderNames.LAST_MODIFIED, new Date(fi.getLastModificationTimeMs()));
            response.headers().set(S3Constants.S3_CONTENT_LENGTH_HEADER,
                fi.isFolder() ? 0 : fi.getLength());

            // Check for the object's ETag
            String entityTag = NettyRestUtils.getEntityTag(fi);
            if (entityTag != null) {
              response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                NettyRestUtils.deserializeContentType(fi.getXAttr()));
            return response;
          } catch (FileDoesNotExistException e) {
            // must be null entity (content length 0) for S3A Filesystem
            HttpResponse response =
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, "0");
            return response;
          } catch (Exception e) {
            throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of HeadObjectTask
}
