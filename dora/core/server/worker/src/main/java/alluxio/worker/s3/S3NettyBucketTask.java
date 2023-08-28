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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.s3.DeleteObjectsRequest;
import alluxio.s3.DeleteObjectsResult;
import alluxio.s3.ListAllMyBucketsResult;
import alluxio.s3.ListBucketOptions;
import alluxio.s3.ListBucketResult;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.net.InetAddresses;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
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
  public HttpResponse continueTask() {
    return NettyRestUtils.call(mHandler.getBucket(), () -> {
      throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED);
    });
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
          } else {
            return new ListObjectsTask(handler, OpType.ListObjects);
          }
        case "PUT":
          return new CreateBucketTask(handler, OpType.CreateBucket);
        case "POST":
          if (handler.getQueryParameter("delete") != null) {
            return new DeleteObjectsTask(handler, OpType.DeleteObjects);
          }
          break;
        case "HEAD":
          if (!StringUtils.isEmpty(handler.getBucket())) {
            return new HeadBucketTask(handler, OpType.HeadBucket);
          }
          break;
        case "DELETE":
          return new DeleteBucketTask(handler, OpType.DeleteBucket);
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
    public HttpResponse continueTask() {
      return NettyRestUtils.call(S3Constants.EMPTY, () -> {
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
              .filter((uri) -> !uri.getName().equals(S3Constants.S3_METADATA_ROOT_DIR))
              // debatable (?) potentially breaks backcompat(?)
              .filter(URIStatus::isFolder)
              .collect(Collectors.toList());
          return new ListAllMyBucketsResult(buckets);
        }
      });
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

    public HttpResponse continueTask() {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        String path = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.getFileSystemForUser(user);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          S3NettyHandler.checkPathIsAlluxioDirectory(userFs, path, auditContext);
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
    }
  } // end of ListObjectsTask

  private static class CreateBucketTask extends S3NettyBucketTask {
    protected CreateBucketTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.getFileSystemForUser(user);
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          if (S3NettyHandler.BUCKET_NAMING_RESTRICTION_ENABLED) {
            Matcher m =
                S3NettyHandler.BUCKET_ADJACENT_DOTS_DASHES_PATTERN.matcher(mHandler.getBucket());
            while (m.find()) {
              if (!m.group().equals("--")) {
                auditContext.setSucceeded(false);
                throw new S3Exception(mHandler.getBucket(), S3ErrorCode.INVALID_BUCKET_NAME);
              }
            }
            if (!S3NettyHandler.BUCKET_VALID_NAME_PATTERN.matcher(mHandler.getBucket()).matches()
                || S3NettyHandler.BUCKET_INVALIDATION_PREFIX_PATTERN.matcher(mHandler.getBucket())
                .matches()
                ||
                S3NettyHandler.BUCKET_INVALID_SUFFIX_PATTERN.matcher(mHandler.getBucket()).matches()
                || InetAddresses.isInetAddress(mHandler.getBucket())) {
              auditContext.setSucceeded(false);
              throw new S3Exception(mHandler.getBucket(), S3ErrorCode.INVALID_BUCKET_NAME);
            }
          }
          try {
            URIStatus status = mHandler.getFsClient().getStatus(new AlluxioURI(bucketPath));
            if (status.isFolder()) {
              if (status.getOwner().equals(user)) {
                // Silently swallow CreateBucket calls on existing buckets for this user
                // - S3 clients may prepend PutObject requests with CreateBucket calls instead of
                //   calling HeadBucket to ensure that the bucket exists
                mHandler.BUCKET_PATH_CACHE.put(bucketPath, true);
                return HttpResponseStatus.OK;
              }
              // Otherwise, this bucket is owned by a different user
              throw new S3Exception(S3ErrorCode.BUCKET_ALREADY_EXISTS);
            }
            // Otherwise, that path exists in Alluxio but is not a directory
            auditContext.setSucceeded(false);
            throw new InvalidPathException("A file already exists at bucket path " + bucketPath);
          } catch (FileDoesNotExistException e) {
            // do nothing, we will create the directory below
          } catch (Exception e) {
            throw NettyRestUtils.toBucketS3Exception(e, bucketPath, auditContext);
          }

          // These permission bits will be inherited by all objects/folders created within
          // the bucket; we don't support custom bucket/object ACLs at the moment
          CreateDirectoryPOptions options =
              CreateDirectoryPOptions.newBuilder()
                  .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE))
                  .setWriteType(S3NettyHandler.S3_WRITE_TYPE)
                  .build();
          try {
            mHandler.getFsClient().createDirectory(new AlluxioURI(bucketPath), options);
            SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
                .setOwner(user)
                .build();
            mHandler.getFsClient().setAttribute(new AlluxioURI(bucketPath), attrPOptions);
          } catch (Exception e) {
            throw NettyRestUtils.toBucketS3Exception(e, bucketPath, auditContext);
          }
          mHandler.BUCKET_PATH_CACHE.put(bucketPath, true);
          return HttpResponseStatus.OK;
        }
      });
    }
  } // end of CreateBucketTask

  private static class DeleteObjectsTask extends S3NettyBucketTask {

    protected DeleteObjectsTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        return null;
      });
    }

    @Override
    public boolean needContent() {
      return true;
    }

    @Override
    public HttpResponse handleContent(HttpContent content) {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.getFileSystemForUser(user);
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          try {
            ByteBuf buf = content.content();
            InputStream readStream = new ByteBufInputStream(buf);
            DeleteObjectsRequest request = new XmlMapper().readerFor(DeleteObjectsRequest.class)
                .readValue(readStream);
            List<DeleteObjectsRequest.DeleteObject> objs = request.getToDelete();
            List<DeleteObjectsResult.DeletedObject> success = new ArrayList<>();
            List<DeleteObjectsResult.ErrorObject> errored = new ArrayList<>();
            objs.sort(Comparator.comparingInt(x -> -1 * x.getKey().length()));
            objs.forEach(obj -> {
              try {
                AlluxioURI uri = new AlluxioURI(bucketPath
                    + AlluxioURI.SEPARATOR + obj.getKey());
                DeletePOptions options = DeletePOptions.newBuilder().build();
                userFs.delete(uri, options);
                DeleteObjectsResult.DeletedObject del = new DeleteObjectsResult.DeletedObject();
                del.setKey(obj.getKey());
                success.add(del);
              } catch (FileDoesNotExistException | DirectoryNotEmptyException e) {
              /*
              FDNE - delete on FDNE should be counted as a success, as there's nothing to do
              DNE - s3 has no concept dirs - if it _is_ a dir, nothing to delete.
               */
                DeleteObjectsResult.DeletedObject del = new DeleteObjectsResult.DeletedObject();
                del.setKey(obj.getKey());
                success.add(del);
              } catch (IOException | AlluxioException e) {
                DeleteObjectsResult.ErrorObject err = new DeleteObjectsResult.ErrorObject();
                err.setKey(obj.getKey());
                err.setMessage(e.getMessage());
                errored.add(err);
              }
            });

            DeleteObjectsResult result = new DeleteObjectsResult();
            if (!request.getQuiet()) {
              result.setDeleted(success);
            }
            result.setErrored(errored);
            return result;
          } catch (IOException e) {
            LOG.debug("Failed to parse DeleteObjects request:", e);
            auditContext.setSucceeded(false);
            return HttpResponseStatus.BAD_REQUEST;
          }
        }
      });
    }
  } // end of DeleteObjectsTask

  private static class HeadBucketTask extends S3NettyBucketTask {
    protected HeadBucketTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.getFileSystemForUser(user);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          mHandler.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
        }
        return HttpResponseStatus.OK;
      });
    }
  } // end of HeadBucketTask

  private static class DeleteBucketTask extends S3NettyBucketTask {

    protected DeleteBucketTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(mHandler.getBucket(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.getFileSystemForUser(user);
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), null)) {
          S3NettyHandler.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          // Delete the bucket.
          DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(Configuration
                  .get(PropertyKey.PROXY_S3_DELETE_TYPE)
                  .equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
              .build();
          try {
            userFs.delete(new AlluxioURI(bucketPath), options);
            mHandler.BUCKET_PATH_CACHE.put(bucketPath, false);
          } catch (Exception e) {
            throw NettyRestUtils.toBucketS3Exception(e, bucketPath, auditContext);
          }
          return HttpResponseStatus.NO_CONTENT;
        }
      });
    }
  } // end of DeleteBucketTask
}
