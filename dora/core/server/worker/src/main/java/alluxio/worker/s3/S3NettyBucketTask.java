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
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.s3.ListAllMyBucketsResult;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;

import io.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * S3 Netty Tasks to handle bucket level or global level request.
 * (only bucket name or no bucket name is provided)
 */
public class S3NettyBucketTask extends S3NettyBaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyBucketTask.class);

  /**
   * Constructs an instance of {@link S3NettyBucketTask}.
   * @param handler
   * @param OPType
   */
  protected S3NettyBucketTask(S3NettyHandler handler, OpType OPType) {
    super(handler, OPType);
  }

  @Override
  public void continueTask() {
    HttpResponse response = NettyRestUtils.call(mHandler.getBucket(), () -> {
      throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED);
    });
    mHandler.processHttpResponse(response);
  }

  /**
   * Factory for getting a S3BucketTask type task.
   */
  public static final class Factory {
    /**
     * Marshall the request and create corresponding bucket level S3 task.
     * @param handler
     * @return S3BucketTask
     */
    public static S3NettyBucketTask create(S3NettyHandler handler) {
      switch (handler.getHttpMethod()) {
        case "GET":
//          if (StringUtils.isEmpty(handler.getBucket())) {
          return new ListBucketsTask(handler, OpType.ListBuckets);
//          } else if (handler.getQueryParameter("tagging") != null) {
//            return new GetBucketTaggingTask(handler, OpType.GetBucketTagging);
//          } else if (handler.getQueryParameter("uploads") != null) {
//            return new ListMultipartUploadsTask(handler, OpType.ListMultipartUploads);
//          } else {
//            return new ListObjectsTask(handler, OpType.ListObjects);
//          }
//        case "PUT":
//          if (handler.getQueryParameter("tagging") != null) {
//            return new PutBucketTaggingTask(handler, OpType.PutBucketTagging);
//          } else {
//            return new CreateBucketTask(handler, OpType.CreateBucket);
//          }
//        case "POST":
//          if (handler.getQueryParameter("delete") != null) {
//            return new DeleteObjectsTask(handler, OpType.DeleteObjects);
//          }
//          break;
//        case "HEAD":
//          if (!StringUtils.isEmpty(handler.getBucket())) {
//            return new HeadBucketTask(handler, OpType.HeadBucket);
//          }
//          break;
//        case "DELETE":
//          if (handler.getQueryParameter("tagging") != null) {
//            return new DeleteBucketTaggingTask(handler, OpType.DeleteBucketTagging);
//          } else {
//            return new DeleteBucketTask(handler, OpType.DeleteBucket);
//          }
        default:
          break;
      }
      return new S3NettyBucketTask(handler, OpType.Unsupported);
    }
  }

  private static class ListBucketsTask extends S3NettyBucketTask {
    protected ListBucketsTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public void continueTask() {
      HttpResponse response = NettyRestUtils.call(S3Constants.EMPTY, () -> {
        final String user = mHandler.getUser();

        List<URIStatus> objects = new ArrayList<>();
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, null, null)) {
          try {
            objects = mHandler.getFsClient().listStatus(new AlluxioURI("/"));
          } catch (AlluxioException | IOException e) {
            throw NettyRestUtils.toBucketS3Exception(e, "/", auditContext);
          }

          final List<URIStatus> buckets = objects.stream()
              .filter((uri) -> uri.getOwner().equals(user))
              // debatable (?) potentially breaks backcompat(?)
              .filter(URIStatus::isFolder)
              .collect(Collectors.toList());
          return new ListAllMyBucketsResult(buckets);
        }
      });
      mHandler.processHttpResponse(response);
    }
  } // end of ListBucketsTask
}
