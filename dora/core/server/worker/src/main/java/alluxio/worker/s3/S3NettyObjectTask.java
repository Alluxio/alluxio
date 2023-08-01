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
import alluxio.PositionReader;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.s3.ChunkedEncodingInputStream;
import alluxio.s3.CopyObjectResult;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.s3.S3RangeSpec;
import alluxio.s3.TaggingData;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.dora.PagedFileReader;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Date;
import javax.ws.rs.core.MediaType;

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
          return new GetObjectTask(handler, OpType.GetObject);
        case "PUT":
          if (handler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
            return new CopyObjectTask(handler, OpType.CopyObject);
          }
          return new S3NettyObjectTask(handler, OpType.Unsupported);
        case "POST":
          break;
        case "HEAD":
          return new HeadObjectTask(handler, OpType.HeadObject);
        case "DELETE":
          return new DeleteObjectTask(handler, OpType.DeleteObject);
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
        String objectPath = PathUtils.concatPath(bucketPath, mHandler.getObject());
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            URIStatus fi = userFs.getStatus(objectUri);
            if (fi.isFolder() && !mHandler.getObject().endsWith(AlluxioURI.SEPARATOR)) {
              throw new FileDoesNotExistException(fi.getPath() + " is a directory");
            }
            HttpResponse response =
                new DefaultHttpResponse(NettyRestUtils.HTTP_VERSION, HttpResponseStatus.OK);
            response.headers()
                .set(HttpHeaderNames.LAST_MODIFIED, new Date(fi.getLastModificationTimeMs()));
            response.headers().set(S3Constants.S3_CONTENT_LENGTH_HEADER,
                fi.isFolder() ? 0 : fi.getLength());

            // Check for the object's ETag
            String entityTag = NettyRestUtils.getEntityTag(fi.getFileInfo());
            if (entityTag != null) {
              response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            response.headers().set(S3Constants.S3_CONTENT_TYPE_HEADER,
                NettyRestUtils.deserializeContentType(fi.getXAttr()));
            return response;
          } catch (FileDoesNotExistException e) {
            // must be null entity (content length 0) for S3A Filesystem
            HttpResponse response =
                new DefaultHttpResponse(NettyRestUtils.HTTP_VERSION, HttpResponseStatus.NOT_FOUND);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, "0");
            return response;
          } catch (Exception e) {
            throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of HeadObjectTask

  private static final class GetObjectTask extends S3NettyObjectTask {

    private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
        Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

    public GetObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(getObjectTaskResource(), () -> {
        final String range = mHandler.getHeaderOrDefault("Range", null);
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        String bucketPath = AlluxioURI.SEPARATOR + mHandler.getBucket();
        String objectPath = PathUtils.concatPath(bucketPath, mHandler.getObject());
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            URIStatus status = userFs.getStatus(objectUri,
                GetStatusPOptions.getDefaultInstance().toBuilder().setIncludeRealContentHash(true)
                    .build());
            S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
            if (!status.isFolder() && status.getLength() > 0) {
              BlockLocationInfo locationInfo =
                  mHandler.getFsClient().getBlockLocations(status).get(0);
              WorkerNetAddress workerNetAddress = locationInfo.getLocations().get(0);
              String currentHost =
                  NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC,
                      Configuration.global());
              if (!workerNetAddress.getHost().equals(currentHost)) {
                final URI uri =
                    new URI("http", null, workerNetAddress.getHost(),
                        Configuration.getInt(PropertyKey.WORKER_REST_PORT),
                        objectPath, null, null);
                LOG.warn("redirect to the uri [{}]", uri);
                HttpResponse response =
                    new DefaultHttpResponse(NettyRestUtils.HTTP_VERSION,
                        HttpResponseStatus.TEMPORARY_REDIRECT);
                response.headers().set(HttpHeaderNames.LOCATION, uri.toString());
                return response;
              }
            }

            AlluxioURI ufsFullPath = mHandler.getUfsPath(objectUri);
            HttpResponse response =
                new DefaultHttpResponse(NettyRestUtils.HTTP_VERSION, HttpResponseStatus.OK);
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
            String contentHash = NettyRestUtils.getEntityTag(status.getFileInfo());
            if (contentHash != null) {
              response.headers().set(S3Constants.S3_ETAG_HEADER, contentHash);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            // TODO(wyy) not support xattr, only return octet-stream.
            response.headers().set(S3Constants.S3_CONTENT_TYPE_HEADER,
                MediaType.APPLICATION_OCTET_STREAM_TYPE);
            response.headers()
                .set(HttpHeaderNames.CONTENT_ENCODING, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            // Check if object had tags, if so we need to return the count
            // in the header "x-amz-tagging-count"
            // TODO(wyy) not support xattr and tagging
            TaggingData tagData = NettyRestUtils.deserializeTags(status.getXAttr());
            if (tagData != null) {
              int taggingCount = tagData.getTagMap().size();
              if (taggingCount > 0) {
                response.headers().set(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
              }
            }
            if (!status.isFolder() && status.getLength() > 0) {
              processGetObject(ufsFullPath.toString(), s3Range, status.getLength(), response);
              return null;
            } else {
              return response;
            }
          } catch (Exception e) {
            throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }

    public void processGetObject(String ufsFullPath, S3RangeSpec range, long objectSize,
                                 HttpResponse response) throws AccessControlException, IOException {
      DataBuffer packet = null;
      long offset = range.getOffset(objectSize);
      long length = range.getLength(objectSize);
      BlockReader blockReader = mHandler.openBlock(ufsFullPath, offset, length);

      // Writes http response to the netty channel before data.
      mHandler.processHttpResponse(response, false);
      try {
        if (mHandler.getFileTransferType() == FileTransferType.TRANSFER) {
          if (blockReader instanceof PagedFileReader) {
            PagedFileReader pagedFileReader = (PagedFileReader) blockReader;
            packet =
                pagedFileReader.getMultipleDataFileChannel(mHandler.getContext().channel(), length);
          }
          if (packet != null) {
            mHandler.processTransferResponse(packet);
          }
        } else {
          mHandler.processMappedResponse(blockReader, objectSize);
        }
      } catch (Exception e) {
        LOG.error("Failed to read data.", e);
        throw e;
      }
    }
  } // end of GetObjectTask

  private static final class CopyObjectTask extends PutObjectTask {

    public CopyObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(getObjectTaskResource(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
        String objectPath = PathUtils.concatPath(bucketPath, object);

        final String copySourceParam = mHandler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER);
        String copySource = !copySourceParam.startsWith(AlluxioURI.SEPARATOR)
            ? AlluxioURI.SEPARATOR + copySourceParam : copySourceParam;

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {

          if (objectPath.endsWith(AlluxioURI.SEPARATOR)) {
            createDirectory(objectPath, userFs, auditContext);
          }
          AlluxioURI objectUri = new AlluxioURI(objectPath);

          // Populate the xattr Map with the metadata tags if provided
//          Map<String, ByteString> xattrMap = new HashMap<>();
//          final String taggingHeader = mHandler.getHeader(S3Constants.S3_TAGGING_HEADER);
//          NettyRestUtils.populateTaggingInXAttr(xattrMap, taggingHeader, auditContext,
//                                                objectPath);

          // populate the xAttr map with the "Content-Type" header
//          final String contentTypeHeader = mHandler.getHeader(S3Constants.S3_CONTENT_TYPE_HEADER);
//          NettyRestUtils.populateContentTypeInXAttr(xattrMap, contentTypeHeader);

          try {
            copySource = URLDecoder.decode(copySource, "UTF-8");
          } catch (UnsupportedEncodingException ex) {
            throw NettyRestUtils.toObjectS3Exception(ex, objectPath, auditContext);
          }
          URIStatus status = null;
          CreateFilePOptions.Builder copyFilePOptionsBuilder = CreateFilePOptions.newBuilder()
              .setRecursive(true)
              .setMode(PMode.newBuilder()
                  .setOwnerBits(Bits.ALL)
                  .setGroupBits(Bits.ALL)
                  .setOtherBits(Bits.NONE)
                  .build())
              .setWriteType(S3NettyHandler.S3_WRITE_TYPE)
              .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
              .setOverwrite(true)
              .setCheckS3BucketPath(true);

          // TODO(wyy) not support tagging and xattr now
          // Handle metadata directive
//          final String metadataDirective = mHandler.getHeader(
//              S3Constants.S3_METADATA_DIRECTIVE_HEADER);
//          if (StringUtils.equals(metadataDirective, S3Constants.Directive.REPLACE.name())
//              && xattrMap.containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
//            copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
//                xattrMap.get(S3Constants.CONTENT_TYPE_XATTR_KEY));
//          } else { // defaults to COPY
//            try {
//              status = userFs.getStatus(new AlluxioURI(copySource));
//              if (status.getFileInfo().getXAttr() != null) {
//                copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
//                    ByteString.copyFrom(status.getFileInfo().getXAttr().getOrDefault(
//                        S3Constants.CONTENT_TYPE_XATTR_KEY,
//                        MediaType.APPLICATION_OCTET_STREAM.getBytes(
//                            S3Constants.HEADER_CHARSET))));
//              }
//            } catch (Exception e) {
//              throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
//            }
//          }

          // Handle tagging directive
//          final String taggingDirective = mHandler.getHeader(
//              S3Constants.S3_TAGGING_DIRECTIVE_HEADER);
//          if (StringUtils.equals(taggingDirective, S3Constants.Directive.REPLACE.name())
//              && xattrMap.containsKey(S3Constants.TAGGING_XATTR_KEY)) {
//            copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
//                xattrMap.get(S3Constants.TAGGING_XATTR_KEY));
//          } else { // defaults to COPY
//            try {
//              if (status == null) {
//                status = userFs.getStatus(new AlluxioURI(copySource));
//              }
//              if (status.getFileInfo().getXAttr() != null
//                  && status.getFileInfo().getXAttr()
//                  .containsKey(S3Constants.TAGGING_XATTR_KEY)) {
//                copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
//                    TaggingData.serialize(S3RestUtils.deserializeTags(status.getXAttr())));
//              }
//            } catch (Exception e) {
//              throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
//            }
//          }

          String entityTag = copyObject(userFs, auditContext,
              objectPath, copySource, copyFilePOptionsBuilder.build());
          return new CopyObjectResult(entityTag, System.currentTimeMillis());
        }
      });
    }
  } // end of CopyObjectTask

  private static class PutObjectTask extends S3NettyObjectTask {
    // For both PutObject and UploadPart

    public PutObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    /**
     * Common function for create object.
     * current logic introduces unhandled race conditions
     * @param objectPath
     * @param userFs
     * @param createFilePOptions
     * @param auditContext
     * @return Response
     * @throws S3Exception
     */
    public HttpResponse createObject(String objectPath, FileSystem userFs,
                                 CreateFilePOptions createFilePOptions, S3AuditContext auditContext)
        throws S3Exception {
      AlluxioURI objectUri = new AlluxioURI(objectPath);
      final String decodedLengthHeader = mHandler.getHeader("x-amz-decoded-content-length");
      final String contentLength = mHandler.getHeader("Content-Length");
      try {
        MessageDigest md5 = MessageDigest.getInstance("MD5");

        // The request body can be in the aws-chunked encoding format, or not encoded at all
        // determine if it's encoded, and then which parts of the stream to read depending on
        // the encoding type.
        boolean isChunkedEncoding = decodedLengthHeader != null;
        long toRead;
        ByteBuf buf = mHandler.getRequestContent();
        InputStream readStream = new ByteBufInputStream(buf);
        if (isChunkedEncoding) {
          toRead = Long.parseLong(decodedLengthHeader);
          readStream = new ChunkedEncodingInputStream(readStream);
        } else {
          toRead = Long.parseLong(contentLength);
        }
        FileOutStream os = userFs.createFile(objectUri, createFilePOptions);
        try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
          long read = ByteStreams.copy(ByteStreams.limit(readStream, toRead),
              digestOutputStream);
          if (read < toRead) {
            throw new IOException(String.format(
                "Failed to read all required bytes from the stream. Read %d/%d",
                read, toRead));
          }
        }

        byte[] digest = md5.digest();
        String base64Digest = BaseEncoding.base64().encode(digest);
        final String contentMD5 = mHandler.getHeader("Content-MD5");
        if (contentMD5 != null && !contentMD5.equals(base64Digest)) {
          // The object may be corrupted, delete the written object and return an error.
          try {
            userFs.delete(objectUri, DeletePOptions.newBuilder().setRecursive(true).build());
          } catch (Exception e2) {
            // intend to continue and return BAD_DIGEST S3Exception.
          }
          throw new S3Exception(objectUri.getPath(), S3ErrorCode.BAD_DIGEST);
        }

        String entityTag = Hex.encodeHexString(digest);
        // persist the ETag via xAttr
        S3NettyHandler.setEntityTag(userFs, objectUri, entityTag);

        HttpResponse response =
            new DefaultHttpResponse(NettyRestUtils.HTTP_VERSION, HttpResponseStatus.OK);
        response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
        return response;
      } catch (Exception e) {
        throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
      }
    }

    /**
     * Common func for copy from a source path to target path.
     * @param userFs
     * @param auditContext
     * @param targetPath
     * @param sourcePath
     * @param copyFilePOption
     * @return entityTag(Etag)
     * @throws S3Exception
     */
    public String copyObject(FileSystem userFs, S3AuditContext auditContext,
                             String targetPath, String sourcePath,
                             CreateFilePOptions copyFilePOption)
        throws S3Exception {
      AlluxioURI objectUri = new AlluxioURI(targetPath);
      if (sourcePath.equals(targetPath)) {
        // do not need to copy a file to itself, unless we are changing file attributes
        // TODO(czhu): support changing metadata via CopyObject to self,
        //  verify for UploadPartCopy
        auditContext.setSucceeded(false);
        throw new S3Exception("Copying an object to itself invalid.",
            targetPath, S3ErrorCode.INVALID_REQUEST);
      }
      URIStatus status;
      try {
        status = userFs.getStatus(new AlluxioURI(sourcePath));
      }  catch (Exception e) {
        throw NettyRestUtils.toObjectS3Exception(e, targetPath, auditContext);
      }
      final String range = mHandler.getHeaderOrDefault(S3Constants.S3_COPY_SOURCE_RANGE, null);
      S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
      PositionReader reader = userFs.openPositionRead(status,
          OpenFilePOptions.getDefaultInstance());
      try (FileOutStream out = userFs.createFile(objectUri, copyFilePOption)) {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        try (DigestOutputStream digestOut = new DigestOutputStream(out, md5)) {
          long position = s3Range.getOffset(status.getLength());
          long length = s3Range.getLength(status.getLength());
          byte[] buf = new byte[8 * Constants.MB];
          int totalRead = 0;
          int currentRead;
          while (totalRead < length) {
            int readlength = Math.min(buf.length, (int) (length - totalRead));
            currentRead = reader.read(position + totalRead, buf, readlength);
            if (currentRead <= 0) {
              break;
            }
            digestOut.write(buf, 0, currentRead);
            totalRead += currentRead;
          }
          byte[] digest = md5.digest();
          String entityTag = Hex.encodeHexString(digest);
          // persist the ETag via xAttr
          S3NettyHandler.setEntityTag(userFs, objectUri, entityTag);
          return entityTag;
        } catch (IOException e) {
          try {
            out.cancel();
          } catch (Throwable t2) {
            e.addSuppressed(t2);
          }
          throw e;
        }
      } catch (Exception e) {
        throw NettyRestUtils.toObjectS3Exception(e, targetPath, auditContext);
      }
    }

    /**
     * Common util func to create directory in alluxio.
     * @param objectPath
     * @param userFs
     * @param auditContext
     * @return Response
     * @throws S3Exception
     */
    public HttpResponseStatus createDirectory(String objectPath, FileSystem userFs,
                                    S3AuditContext auditContext)
        throws S3Exception {
      // Need to create a folder
      // TODO(czhu): verify S3 behaviour when ending an object path with a delimiter
      // - this is a convenience method for the Alluxio fs which does not have a
      //   direct counterpart for S3, since S3 does not have "folders" as actual objects
      try {
        CreateDirectoryPOptions dirOptions = CreateDirectoryPOptions.newBuilder()
            .setRecursive(true)
            .setMode(PMode.newBuilder()
                .setOwnerBits(Bits.ALL)
                .setGroupBits(Bits.ALL)
                .setOtherBits(Bits.NONE).build())
            .setAllowExists(true)
            .setCheckS3BucketPath(true)
            .build();
        userFs.createDirectory(new AlluxioURI(objectPath), dirOptions);
      } catch (FileAlreadyExistsException e) {
        // ok if directory already exists the user wanted to create it anyway
        LOG.warn("attempting to create dir which already exists");
      } catch (IOException | AlluxioException e) {
        throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
      }
      return HttpResponseStatus.OK;
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(getObjectTaskResource(), () -> {
        // PutObject / UploadPart ...
        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);

        try (S3AuditContext auditContext =
                 mHandler.createAuditContext(mOPType.name(), user, bucket, object)) {
          String objectPath = PathUtils.concatPath(bucketPath, object);

          if (objectPath.endsWith(AlluxioURI.SEPARATOR)) {
            return createDirectory(objectPath, userFs, auditContext);
          }

          // TODO(wyy) populate xattr map
//          // Populate the xattr Map with the metadata tags if provided
//          Map<String, ByteString> xattrMap = new HashMap<>();
//          final String taggingHeader = mHandler.getHeader(S3Constants.S3_TAGGING_HEADER);
//          NettyRestUtils.populateTaggingInXAttr(xattrMap, taggingHeader,
//          auditContext, objectPath);
//
//          // populate the xAttr map with the "Content-Type" header
//          final String contentTypeHeader = mHandler.getHeader(S3Constants.S3_CONTENT_TYPE_HEADER);
//          NettyRestUtils.populateContentTypeInXAttr(xattrMap, contentTypeHeader);

          CreateFilePOptions filePOptions =
              CreateFilePOptions.newBuilder()
                  .setRecursive(true)
                  .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE).build())
                  .setWriteType(Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT,
                      WriteType.class).toProto())
                  // TODO(wyy) not support xattr and tagging now
//                  .putAllXattr(xattrMap).setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                  .setOverwrite(true)
                  .setCheckS3BucketPath(true)
                  .build();
          return createObject(objectPath, userFs, filePOptions, auditContext);
        }
      });
    }
  } // end of PutObjectTask

  private static final class DeleteObjectTask extends S3NettyObjectTask {

    public DeleteObjectTask(S3NettyHandler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public HttpResponse continueTask() {
      return NettyRestUtils.call(getObjectTaskResource(), () -> {
        // DeleteObjectTask ...
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = mHandler.createFileSystemForUser(user);
        String bucketPath = NettyRestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        // Delete the object.
        String objectPath = PathUtils.concatPath(bucketPath, mHandler.getObject());
        DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(Configuration
                .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
            .build();
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            "deleteObject", user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            userFs.delete(new AlluxioURI(objectPath), options);
          } catch (FileDoesNotExistException | DirectoryNotEmptyException e) {
            // intentionally do nothing, this is ok. It should result in a 204 error
            // This is the same response behavior as AWS's S3.
          } catch (Exception e) {
            throw NettyRestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return HttpResponseStatus.NO_CONTENT;
      });
    }
  } // end of DeleteObjectTask
}
