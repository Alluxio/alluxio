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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.s3.S3RangeSpec;
import alluxio.s3.TaggingData;
import alluxio.wire.FileInfo;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedFileReader;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * S3 Netty Tasks to handle object level request.
 * (bucket and object name provided in the request)
 */
public class S3NettyObjectTask extends S3NettyBaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyObjectTask.class);

  protected S3NettyObjectTask(S3NettyHandler handler, OpType opType) {
    super(handler, opType);
  }

  @Override
  public void continueTask() {
    HttpResponse response = NettyRestUtils.call(mHandler.getBucket(), () -> {
      throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED);
    });
    mHandler.processHttpResponse(response);
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
          return new GetObjectTask(handler, OpType.GetObject);
        case "POST":
          break;
        case "HEAD":
          return new HeadObjectTask(handler, OpType.HeadObject);
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
    public void continueTask() {
      HttpResponse httpResponse = NettyRestUtils.call(getObjectTaskResource(), () -> {
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
            FileInfo fi = doraWorker.getFileInfo(ufsFullPath.toString(),
                GetStatusPOptions.getDefaultInstance());
//            }
//            URIStatus status = userFs.getStatus(objectUri);
            if (fi.isFolder() && !mHandler.getObject().endsWith(AlluxioURI.SEPARATOR)) {
              throw new FileDoesNotExistException(fi.getPath() + " is a directory");
            }
            HttpResponse response =
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers()
                .set(HttpHeaderNames.LAST_MODIFIED, new Date(fi.getLastModificationTimeMs()));
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
      mHandler.processHttpResponse(httpResponse);
    }
  } // end of HeadObjectTask

  private static final class GetObjectTask extends S3NettyObjectTask {

    private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
        Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

    public GetObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public void continueTask() {
      HttpResponse httpResponse = NettyRestUtils.call(getObjectTaskResource(), () -> {
        final String range = mHandler.getHeaderOrDefault("Range", null);
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        String bucketPath = AlluxioURI.SEPARATOR + mHandler.getBucket();
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            AlluxioURI ufsFullPath =
                ((DoraCacheFileSystem) userFs).convertAlluxioPathToUFSPath(objectUri);
            DoraWorker doraWorker = mHandler.getDoraWorker();
            FileInfo status = doraWorker.getFileInfo(ufsFullPath.toString(),
                GetStatusPOptions.getDefaultInstance());
            S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);

            HttpResponse response =
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers()
                .set(HttpHeaderNames.LAST_MODIFIED, new Date(status.getLastModificationTimeMs()));
            response.headers().set(S3Constants.S3_CONTENT_LENGTH_HEADER,
                status.isFolder() ? 0 : status.getLength());

            // Check range
            if (s3Range.isValid()) {
              response.setStatus(HttpResponseStatus.PARTIAL_CONTENT);
              response.headers()
                  .set(S3Constants.S3_ACCEPT_RANGES_HEADER, S3Constants.S3_ACCEPT_RANGES_VALUE);
              response.headers().set(S3Constants.S3_CONTENT_RANGE_HEADER,
                  s3Range.getRealRange(status.getLength()));
            }

            // Check for the object's ETag
            String entityTag = NettyRestUtils.getEntityTag(status);
            if (entityTag != null) {
              response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                NettyRestUtils.deserializeContentType(status.getXAttr()));

            // Check if object had tags, if so we need to return the count
            // in the header "x-amz-tagging-count"
            TaggingData tagData = NettyRestUtils.deserializeTags(status.getXAttr());
            if (tagData != null) {
              int taggingCount = tagData.getTagMap().size();
              if (taggingCount > 0) {
                response.headers().set(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
              }
            }
            if (status.isFolder() && status.getLength() > 0) {
              processGetObject(ufsFullPath.toString(), s3Range, status.getLength(), response);
            } else {
              mHandler.processHttpResponse(response, false);
            }
            return response;
          } catch (Exception e) {
            throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
      if (mHandler.getContext().channel().isOpen()) {
        // only process error http response here
        mHandler.processHttpResponse(httpResponse);
      }
    }

    public void processGetObject(String ufsFullPath, S3RangeSpec range, long objectSize,
                                 HttpResponse response) throws AccessControlException, IOException {
      DataBuffer packet = null;
      long offset = range.getOffset(objectSize);
      long length = range.getLength(objectSize);
      BlockReader blockReader = mHandler.openBlock(ufsFullPath, offset, length);

      try {
        // Writes http response to the netty channel before data.
        mHandler.processHttpResponse(response, false);
        if (mHandler.getFileTransferType() == FileTransferType.TRANSFER) {
          if (blockReader instanceof LocalFileBlockReader) {
            packet =
                new DataFileChannel(new File(((LocalFileBlockReader) blockReader).getFilePath()),
                    offset, length);
          } else if (blockReader instanceof PagedFileReader) {
            PagedFileReader pagedFileReader = (PagedFileReader) blockReader;
            packet =
                pagedFileReader.getMultipleDataFileChannel(mHandler.getContext().channel(), length);
          }
        } else {
          mHandler.processMappedResponse(blockReader);
        }
      } catch (Exception e) {
        LOG.error("Failed to read data.", e);
        throw e;
      }

      if (packet != null) {
        mHandler.processTransferResponse(packet);
      }
      mHandler.getContext().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
          .addListener(ChannelFutureListener.CLOSE);
    }
  } // end of GetObjectTask
}
