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
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.s3.ListAllMyBucketsResult;
import alluxio.s3.ListBucketOptions;
import alluxio.s3.ListBucketResult;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;

import io.netty.handler.codec.http.HttpResponse;
import org.apache.commons.lang3.StringUtils;
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
          if (StringUtils.isEmpty(handler.getBucket())) {
            return new ListBucketsTask(handler, OpType.ListBuckets);
//          } else if (handler.getQueryParameter("tagging") != null) {
//            return new GetBucketTaggingTask(handler, OpType.GetBucketTagging);
//          } else if (handler.getQueryParameter("uploads") != null) {
//            return new ListMultipartUploadsTask(handler, OpType.ListMultipartUploads);
          } else {
            return new ListObjectsTask(handler, OpType.ListObjects);
          }
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

  private static class ListObjectsTask extends S3NettyBucketTask {
    protected ListObjectsTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    private String normalizeS3Prefix(String prefix, char delimiter) {
      if (prefix != null) {
        int pos = prefix.lastIndexOf(delimiter);
        if (pos >= 0) {
          return prefix.substring(0, pos + 1);
        }
      }
      return S3Constants.EMPTY;
    }

    private String parsePathWithDelimiter(String bucketPath, String prefix, String delimiter)
        throws S3Exception {
      // TODO(czhu): allow non-"/" delimiters
      // Alluxio only support use / as delimiter
      if (!delimiter.equals(AlluxioURI.SEPARATOR)) {
        throw new S3Exception(bucketPath, new S3ErrorCode(
            S3ErrorCode.PRECONDITION_FAILED.getCode(),
            "Alluxio S3 API only support / as delimiter.",
            S3ErrorCode.PRECONDITION_FAILED.getStatus()));
      }
      char delim = AlluxioURI.SEPARATOR.charAt(0);
      String normalizedBucket =
          bucketPath.replace(S3Constants.BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
      String normalizedPrefix = normalizeS3Prefix(prefix, delim);

      if (!normalizedPrefix.isEmpty() && !normalizedPrefix.startsWith(AlluxioURI.SEPARATOR)) {
        normalizedPrefix = AlluxioURI.SEPARATOR + normalizedPrefix;
      }
      return normalizedBucket + normalizedPrefix;
    }

    public void continueTask() {
      HttpResponse response = NettyRestUtils.call(mHandler.getBucket(), () -> {
        String path = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          // TODO(wyy) checkPathIsAlluxioDirectory
//          S3RestUtils.checkPathIsAlluxioDirectory(userFs, path, auditContext,
//              mHandler.BUCKET_PATH_CACHE);
          String markerParam = mHandler.getQueryParameter("marker");
          String maxKeysParam = mHandler.getQueryParameter("max-keys");
          String prefixParam = mHandler.getQueryParameter("prefix");
          String delimiterParam = mHandler.getQueryParameter("delimiter");
          String encodingTypeParam = mHandler.getQueryParameter("encoding-type");
          String listTypeParam = mHandler.getQueryParameter("list-type");
          String continuationTokenParam = mHandler.getQueryParameter("continuation-token");
          String startAfterParam = mHandler.getQueryParameter("start-after");

          int maxKeys = maxKeysParam == null
              ? ListBucketOptions.DEFAULT_MAX_KEYS : Integer.parseInt(maxKeysParam);
          Integer listType = listTypeParam == null ? null : Integer.parseInt(listTypeParam);
          ListBucketOptions listBucketOptions = ListBucketOptions.defaults()
              .setMarker(markerParam)
              .setPrefix(prefixParam)
              .setMaxKeys(maxKeys)
              .setDelimiter(delimiterParam)
              .setEncodingType(encodingTypeParam)
              .setListType(listType)
              .setContinuationToken(continuationTokenParam)
              .setStartAfter(startAfterParam);

          List<URIStatus> children;
          try {
            // TODO(czhu): allow non-"/" delimiters by parsing the prefix & delimiter pair to
            //             determine what directory to list the contents of
            //             only list the direct children if delimiter is not null
            if (StringUtils.isNotEmpty(delimiterParam)) {
              if (prefixParam == null) {
                path = parsePathWithDelimiter(path, S3Constants.EMPTY, delimiterParam);
              } else {
                path = parsePathWithDelimiter(path, prefixParam, delimiterParam);
              }
              children = userFs.listStatus(new AlluxioURI(path));
            } else {
              if (prefixParam != null) {
                path = parsePathWithDelimiter(path, prefixParam, AlluxioURI.SEPARATOR);
              }
              ListStatusPOptions options = ListStatusPOptions.newBuilder()
                  .setRecursive(true).build();
              children = userFs.listStatus(new AlluxioURI(path), options);
            }
          } catch (FileDoesNotExistException e) {
            // Since we've called S3RestUtils.checkPathIsAlluxioDirectory() on the bucket path
            // already, this indicates that the prefix was unable to be found in the Alluxio FS
            children = new ArrayList<>();
          } catch (IOException | AlluxioException e) {
            throw NettyRestUtils.toBucketS3Exception(e, mHandler.getBucket(), auditContext);
          }
          return new ListBucketResult(
              mHandler.getBucket(),
              children,
              listBucketOptions);
        } // end try-with-resources block
      });
      mHandler.processHttpResponse(response);
    }
  } // end of ListObjectsTask
}
