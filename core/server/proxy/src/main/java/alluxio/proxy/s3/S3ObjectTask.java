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
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.S3SyntaxOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.File;
import alluxio.util.ThreadUtils;
import alluxio.web.ProxyWebServer;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * S3 Tasks to handle object level request.
 * (bucket and object name provided in the request)
 */
public class S3ObjectTask extends S3BaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(S3ObjectTask.class);

  protected S3ObjectTask(S3Handler handler, OpType opType) {
    super(handler, opType);
  }

  @Override
  public Response continueTask() {
    return S3RestUtils.call(mHandler.getBucket(), () -> {
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
    public static S3ObjectTask create(S3Handler handler) {
      switch (handler.getHTTPVerb()) {
        case "GET":
          if (handler.getQueryParameter("uploadId") != null) {
            return new ListPartsTask(handler, OpType.ListParts);
          } else if (handler.getQueryParameter("tagging") != null) {
            return new GetObjectTaggingTask(handler, OpType.GetObjectTagging);
          } else {
            return new GetObjectTask(handler, OpType.GetObject);
          }
        case "PUT":
          if (handler.getQueryParameter("tagging") != null) {
            return new PutObjectTaggingTask(handler, OpType.PutObjectTagging);
          } else if (handler.getQueryParameter("uploadId") != null) {
            if (handler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
              return new UploadPartTask(handler, OpType.UploadPartCopy);
            }
            return new UploadPartTask(handler, OpType.UploadPart);
          } else {
            if (handler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
              return new CopyObjectTask(handler, OpType.CopyObject);
            }
            return new PutObjectTask(handler, OpType.PutObject);
          }
        case "POST":
          if (handler.getQueryParameter("uploads") != null) {
            return new CreateMultipartUploadTask(handler, OpType.CreateMultipartUpload);
          } else if (handler.getQueryParameter("uploadId") != null) {
            return new CompleteMultipartUploadTask(handler, OpType.CompleteMultipartUpload);
          }
          break;
        case "HEAD":
          return new HeadObjectTask(handler, OpType.HeadObject);
        case "DELETE":
          if (handler.getQueryParameter("uploadId") != null) {
            return new AbortMultipartUploadTask(handler, OpType.AbortMultipartUpload);
          } else if (handler.getQueryParameter("tagging") != null) {
            return new DeleteObjectTaggingTask(handler, OpType.DeleteObjectTagging);
          } else {
            return new DeleteObjectTask(handler, OpType.DeleteObject);
          }
        default:
          return new S3ObjectTask(handler, OpType.Unsupported);
      }
      return new S3ObjectTask(handler, OpType.Unsupported);
    }
  }

  private static final class ListPartsTask extends S3ObjectTask {

    public ListPartsTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        final String uploadId = mHandler.getQueryParameter("uploadId");
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);

          AlluxioURI tmpDir = new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(
              bucketPath, mHandler.getObject(), uploadId));
          try {
            S3RestUtils.checkStatusesForUploadId(mHandler.getMetaFS(), userFs, tmpDir, uploadId);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception((e instanceof FileDoesNotExistException)
                    ? new S3Exception(mHandler.getObject(), S3ErrorCode.NO_SUCH_UPLOAD) : e,
                mHandler.getObject(), auditContext);
          }

          try {
            List<URIStatus> statuses = userFs.listStatus(tmpDir);
            statuses.sort(new S3RestUtils.URIStatusNameComparator());

            List<ListPartsResult.Part> parts = new ArrayList<>();
            for (URIStatus status : statuses) {
              parts.add(ListPartsResult.Part.fromURIStatus(status));
            }

            ListPartsResult result = new ListPartsResult();
            result.setBucket(bucketPath);
            result.setKey(mHandler.getObject());
            result.setUploadId(uploadId);
            result.setParts(parts);
            return result;
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, tmpDir.getPath(), auditContext);
          }
        }
      });
    }
  } // end of ListPartsTask

  private static final class GetObjectTaggingTask extends S3ObjectTask {

    public GetObjectTaggingTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI uri = new AlluxioURI(objectPath);
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          try {
            TaggingData tagData = S3RestUtils.deserializeTags(userFs.getStatus(uri).getXAttr());
            LOG.debug("GetObjectTagging tagData={}", tagData);
            return tagData != null ? tagData : new TaggingData();
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of GetObjectTaggingTask

  private static final class PutObjectTaggingTask extends S3ObjectTask {

    private PutObjectTaggingTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
          AlluxioURI objectUri = new AlluxioURI(objectPath);
          TaggingData tagData = null;
          try {
            tagData = new XmlMapper().readerFor(TaggingData.class)
                .readValue(mHandler.getInputStream());
          } catch (IOException e) {
            if (e.getCause() instanceof S3Exception) {
              throw S3RestUtils.toObjectS3Exception((S3Exception) e.getCause(), objectPath,
                  auditContext);
            }
            auditContext.setSucceeded(false);
            throw new S3Exception(e, objectPath, S3ErrorCode.MALFORMED_XML);
          }
          LOG.debug("PutObjectTagging tagData={}", tagData);
          Map<String, ByteString> xattrMap = new HashMap<>();
          if (tagData != null) {
            try {
              xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
            } catch (Exception e) {
              throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
            }
          }
          try {
            SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
                .putAllXattr(xattrMap)
                .setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
                .build();
            userFs.setAttribute(objectUri, attrPOptions);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
          return Response.ok().build();
        }
      });
    }
  } // end of PutObjectTaggingTask

  private static final class GetObjectTask extends S3ObjectTask {

    public GetObjectTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        final String range = mHandler.getHeaderOrDefault("Range", null);
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            URIStatus status = userFs.getStatus(objectUri);
            FileInStream is = userFs.openFile(objectUri);
            S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
            RangeFileInStream ris = RangeFileInStream.Factory.create(
                is, status.getLength(), s3Range);

            Response.ResponseBuilder res = Response.ok(ris, MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .lastModified(new Date(status.getLastModificationTimeMs()))
                .header(S3Constants.S3_CONTENT_LENGTH_HEADER,
                    s3Range.getLength(status.getLength()));

            // Check for the object's ETag
            String entityTag = S3RestUtils.getEntityTag(status);
            if (entityTag != null) {
              res.header(S3Constants.S3_ETAG_HEADER, entityTag);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            res.type(S3RestUtils.deserializeContentType(status.getXAttr()));

            // Check if object had tags, if so we need to return the count
            // in the header "x-amz-tagging-count"
            TaggingData tagData = S3RestUtils.deserializeTags(status.getXAttr());
            if (tagData != null) {
              int taggingCount = tagData.getTagMap().size();
              if (taggingCount > 0) {
                res.header(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
              }
            }
            return res.build();
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of GetObjectTask

  private static final class HeadObjectTask extends S3ObjectTask {

    public HeadObjectTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI objectUri = new AlluxioURI(objectPath);

        try (S3AuditContext auditContext = mHandler.createAuditContext(
            mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
          try {
            URIStatus status = userFs.getStatus(objectUri);
            if (status.isFolder() && !mHandler.getObject().endsWith(AlluxioURI.SEPARATOR)) {
              throw new FileDoesNotExistException(status.getPath() + " is a directory");
            }
            Response.ResponseBuilder res = Response.ok()
                .lastModified(new Date(status.getLastModificationTimeMs()))
                .header(S3Constants.S3_CONTENT_LENGTH_HEADER,
                    status.isFolder() ? 0 : status.getLength());

            // Check for the object's ETag
            String entityTag = S3RestUtils.getEntityTag(status);
            if (entityTag != null) {
              res.header(S3Constants.S3_ETAG_HEADER, entityTag);
            } else {
              LOG.debug("Failed to find ETag for object: " + objectPath);
            }

            // Check if the object had a specified "Content-Type"
            res.type(S3RestUtils.deserializeContentType(status.getXAttr()));
            return res.build();
          } catch (FileDoesNotExistException e) {
            // must be null entity (content length 0) for S3A Filesystem
            return Response.status(404).entity(null).header("Content-Length", "0").build();
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of HeadObjectTask

  private static final class CopyObjectTask extends PutObjectTask {

    public CopyObjectTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

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
          Map<String, ByteString> xattrMap = new HashMap<>();
          final String taggingHeader = mHandler.getHeader(S3Constants.S3_TAGGING_HEADER);
          S3RestUtils.populateTaggingInXAttr(xattrMap, taggingHeader, auditContext, objectPath);

          // populate the xAttr map with the "Content-Type" header
          final String contentTypeHeader = mHandler.getHeader(S3Constants.S3_CONTENT_TYPE_HEADER);
          S3RestUtils.populateContentTypeInXAttr(xattrMap, contentTypeHeader);

          CreateFilePOptions filePOptions =
              CreateFilePOptions.newBuilder()
                  .setRecursive(true)
                  .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE).build())
                  .setWriteType(S3RestUtils.getS3WriteType())
                  .putAllXattr(xattrMap)
                  .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                  .build();

          try {
            copySource = URLDecoder.decode(copySource, "UTF-8");
          } catch (UnsupportedEncodingException ex) {
            throw S3RestUtils.toObjectS3Exception(ex, objectPath, auditContext);
          }
          URIStatus status = null;
          CreateFilePOptions.Builder copyFilePOptionsBuilder = CreateFilePOptions.newBuilder()
              .setRecursive(true)
              .setMode(PMode.newBuilder()
                  .setOwnerBits(Bits.ALL)
                  .setGroupBits(Bits.ALL)
                  .setOtherBits(Bits.NONE).build());

          // Handle metadata directive
          final String metadataDirective = mHandler.getHeader(
              S3Constants.S3_METADATA_DIRECTIVE_HEADER);
          if (StringUtils.equals(metadataDirective, S3Constants.Directive.REPLACE.name())
              && filePOptions.getXattrMap().containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
            copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
                filePOptions.getXattrMap().get(S3Constants.CONTENT_TYPE_XATTR_KEY));
          } else { // defaults to COPY
            try {
              status = userFs.getStatus(new AlluxioURI(copySource));
              if (status.getFileInfo().getXAttr() != null) {
                copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
                    ByteString.copyFrom(status.getFileInfo().getXAttr().getOrDefault(
                        S3Constants.CONTENT_TYPE_XATTR_KEY,
                        MediaType.APPLICATION_OCTET_STREAM.getBytes(S3Constants.HEADER_CHARSET))));
              }
            } catch (Exception e) {
              throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
            }
          }

          // Handle tagging directive
          final String taggingDirective = mHandler.getHeader(
              S3Constants.S3_TAGGING_DIRECTIVE_HEADER);
          if (StringUtils.equals(taggingDirective, S3Constants.Directive.REPLACE.name())
              && filePOptions.getXattrMap().containsKey(S3Constants.TAGGING_XATTR_KEY)) {
            copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
                filePOptions.getXattrMap().get(S3Constants.TAGGING_XATTR_KEY));
          } else { // defaults to COPY
            try {
              if (status == null) {
                status = userFs.getStatus(new AlluxioURI(copySource));
              }
              if (status.getFileInfo().getXAttr() != null
                  && status.getFileInfo().getXAttr()
                  .containsKey(S3Constants.TAGGING_XATTR_KEY)) {
                copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
                    TaggingData.serialize(S3RestUtils.deserializeTags(status.getXAttr())));
              }
            } catch (Exception e) {
              throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
            }
          }

          String entityTag = copyObject(userFs, auditContext,
              objectPath, copySource, copyFilePOptionsBuilder.build());
          return new CopyObjectResult(entityTag, System.currentTimeMillis());
        }
      });
    }
  } // end of CopyObjectTask

  private static class PutObjectTask extends S3ObjectTask {
    // For both PutObject and UploadPart

    public PutObjectTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    /**
     * Common function for create object.
     * TODO(lucy) needs to change the central logic here of how we do overwrite
     * current logic introduces unhandled race conditions
     * @param objectPath
     * @param userFs
     * @param createFilePOptions
     * @param auditContext
     * @return Response
     * @throws S3Exception
     */
    public Response createObject(String objectPath, FileSystem userFs,
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
        InputStream readStream = mHandler.getInputStream();
        if (isChunkedEncoding) {
          toRead = Long.parseLong(decodedLengthHeader);
          readStream = new ChunkedEncodingInputStream(readStream);
        } else {
          toRead = Long.parseLong(contentLength);
        }
        try {
          S3RestUtils.deleteExistObject(userFs, objectUri);
        } catch (IOException | AlluxioException e) {
          throw S3RestUtils.toObjectS3Exception(e, objectUri.getPath(), auditContext);
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
        // TODO(czhu): try to compute the ETag prior to creating the file
        //  to reduce total RPC RTT
        S3RestUtils.setEntityTag(userFs, objectUri, entityTag);
        return Response.ok().header(S3Constants.S3_ETAG_HEADER, entityTag).build();
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
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
    public Response createDirectory(String objectPath, FileSystem userFs,
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
            .build();
        userFs.createDirectory(new AlluxioURI(objectPath), dirOptions);
      } catch (FileAlreadyExistsException e) {
        // ok if directory already exists the user wanted to create it anyway
        LOG.warn("attempting to create dir which already exists");
      } catch (IOException | AlluxioException e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
      }
      return Response.ok().build();
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
      try {
        S3RestUtils.deleteExistObject(userFs, objectUri);
      } catch (IOException | AlluxioException e) {
        throw S3RestUtils.toObjectS3Exception(e, objectUri.getPath(), auditContext);
      }
      try (FileInStream in = userFs.openFile(new AlluxioURI(sourcePath));
           FileOutStream out = userFs.createFile(objectUri, copyFilePOption)) {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        try (DigestOutputStream digestOut = new DigestOutputStream(out, md5)) {
          IOUtils.copyLarge(in, digestOut, new byte[8 * Constants.MB]);
          byte[] digest = md5.digest();
          String entityTag = Hex.encodeHexString(digest);
          // persist the ETag via xAttr
          // TODO(czhu): compute the ETag prior to creating the file to reduce total RPC RTT
          S3RestUtils.setEntityTag(userFs, objectUri, entityTag);
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
        throw S3RestUtils.toObjectS3Exception(e, targetPath, auditContext);
      }
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // PutObject / UploadPart ...
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(object, "required 'object' parameter is missing");
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);

        try (S3AuditContext auditContext =
                 mHandler.createAuditContext(mOPType.name(), user, bucket, object)) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

          if (objectPath.endsWith(AlluxioURI.SEPARATOR)) {
            return createDirectory(objectPath, userFs, auditContext);
          }
          AlluxioURI objectUri = new AlluxioURI(objectPath);

          // Populate the xattr Map with the metadata tags if provided
          Map<String, ByteString> xattrMap = new HashMap<>();
          final String taggingHeader = mHandler.getHeader(S3Constants.S3_TAGGING_HEADER);
          S3RestUtils.populateTaggingInXAttr(xattrMap, taggingHeader, auditContext, objectPath);

          // populate the xAttr map with the "Content-Type" header
          final String contentTypeHeader = mHandler.getHeader(S3Constants.S3_CONTENT_TYPE_HEADER);
          S3RestUtils.populateContentTypeInXAttr(xattrMap, contentTypeHeader);

          CreateFilePOptions filePOptions =
              CreateFilePOptions.newBuilder()
                  .setRecursive(true)
                  .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE).build())
                  .setWriteType(S3RestUtils.getS3WriteType())
                  .putAllXattr(xattrMap).setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                  .build();
          return createObject(objectPath, userFs, filePOptions, auditContext);
        }
      });
    }
  } // end of PutObjectTask

  private static final class UploadPartTask extends PutObjectTask {

    public UploadPartTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // UploadPart related params
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);

        final String partNumberStr = mHandler.getQueryParameter("partNumber");
        Integer partNumber = null;
        if (StringUtils.isNotEmpty(partNumberStr)) {
          try {
            partNumber = Integer.parseInt(partNumberStr);
          } catch (Exception ex) {
            return new S3Exception(ex, object, S3ErrorCode.INVALID_ARGUMENT);
          }
        }
        final String uploadId = mHandler.getQueryParameter("uploadId");
        Preconditions.checkNotNull(partNumber, "required 'partNumber' parameter is missing");
        Preconditions.checkNotNull(partNumber, "required 'uploadId' parameter is missing");

        try (S3AuditContext auditContext =
                 mHandler.createAuditContext(mOPType.name(), user, bucket, object)) {
          // This object is part of a multipart upload, should be uploaded into the temporary
          // directory first.
          String tmpDir =
              S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId);
          try {
            S3RestUtils.checkStatusesForUploadId(
                mHandler.getMetaFS(), userFs, new AlluxioURI(tmpDir), uploadId);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception((e instanceof FileDoesNotExistException)
                    ? new S3Exception(object, S3ErrorCode.NO_SUCH_UPLOAD) : e,
                object, auditContext);
          }
          String objectPath = tmpDir + AlluxioURI.SEPARATOR + partNumber;
          // eg: /bucket/folder/object_<uploadId>/<partNumber>

          // UploadPartCopy with source from another object
          if (mHandler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER) != null) {
            final String copySourceParam = mHandler.getHeader(S3Constants.S3_COPY_SOURCE_HEADER);
            String copySource = !copySourceParam.startsWith(AlluxioURI.SEPARATOR)
                ? AlluxioURI.SEPARATOR + copySourceParam : copySourceParam;
            try {
              copySource = URLDecoder.decode(copySource, "UTF-8");
            } catch (UnsupportedEncodingException ex) {
              throw S3RestUtils.toObjectS3Exception(ex, objectPath, auditContext);
            }
            CreateFilePOptions.Builder copyFilePOptionsBuilder = CreateFilePOptions.newBuilder()
                .setRecursive(true)
                .setMode(PMode.newBuilder()
                    .setOwnerBits(Bits.ALL)
                    .setGroupBits(Bits.ALL)
                    .setOtherBits(Bits.NONE).build());
            String entityTag = copyObject(userFs, auditContext, objectPath,
                copySource, copyFilePOptionsBuilder.build());
            return new CopyPartResult(entityTag);
          }
          // UploadPart with source from http body
          CreateFilePOptions filePOptions =
              CreateFilePOptions.newBuilder()
                  .setRecursive(true)
                  .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE).build())
                  .setWriteType(S3RestUtils.getS3WriteType())
                  .build();
          return createObject(objectPath, userFs, filePOptions, auditContext);
        }
      });
    }
  } // end of UploadPartTask

  private static final class CreateMultipartUploadTask extends S3ObjectTask {

    public CreateMultipartUploadTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // CreateMultipartUploadTask ...
        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

        // Populate the xattr Map with the metadata tags if provided
        Map<String, ByteString> xattrMap = new HashMap<>();

        TaggingData tagData = null;
        final String taggingHeader = mHandler.getHeader(S3Constants.S3_TAGGING_HEADER);
        final String contentTypeHeader = mHandler.getHeader(S3Constants.S3_CONTENT_TYPE_HEADER);
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            "initiateMultipartUpload", user, bucket, object)) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          if (taggingHeader != null) { // Parse the tagging header if it exists
            try {
              tagData = S3RestUtils.deserializeTaggingHeader(
                  taggingHeader, S3Handler.MAX_HEADER_METADATA_SIZE);
              xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
            } catch (S3Exception e) {
              auditContext.setSucceeded(false);
              throw e; // rethrow
            } catch (IllegalArgumentException e) {
              if (e.getCause() instanceof S3Exception) {
                throw S3RestUtils.toObjectS3Exception((S3Exception) e.getCause(), objectPath,
                    auditContext);
              }
              throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
            } catch (Exception e) {
              throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
            }
            LOG.debug("InitiateMultipartUpload tagData={}", tagData);
          }

          try {
            // Find an unused UUID
            String uploadId;
            do {
              uploadId = UUID.randomUUID().toString();
            } while (mHandler.getMetaFS().exists(
                new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId))));

            // Create the directory containing the upload parts
            AlluxioURI multipartTemporaryDir = new AlluxioURI(
                S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId));
            userFs.createDirectory(multipartTemporaryDir, CreateDirectoryPOptions.newBuilder()
                .setRecursive(true)
                .setMode(PMode.newBuilder()
                    .setOwnerBits(Bits.ALL)
                    .setGroupBits(Bits.ALL)
                    .setOtherBits(Bits.NONE).build())
                .setWriteType(S3RestUtils.getS3WriteType()).build());

            // Create the Alluxio multipart upload metadata file
            if (contentTypeHeader != null) {
              xattrMap.put(S3Constants.CONTENT_TYPE_XATTR_KEY,
                  ByteString.copyFrom(contentTypeHeader, S3Constants.HEADER_CHARSET));
            }
            xattrMap.put(S3Constants.UPLOADS_BUCKET_XATTR_KEY,
                ByteString.copyFrom(mHandler.getBucket(), S3Constants.XATTR_STR_CHARSET));
            xattrMap.put(S3Constants.UPLOADS_OBJECT_XATTR_KEY,
                ByteString.copyFrom(mHandler.getObject(), S3Constants.XATTR_STR_CHARSET));
            xattrMap.put(S3Constants.UPLOADS_FILE_ID_XATTR_KEY, ByteString.copyFrom(
                Longs.toByteArray(userFs.getStatus(multipartTemporaryDir).getFileId())));
            mHandler.getMetaFS().createFile(
                new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)),
                CreateFilePOptions.newBuilder()
                    .setRecursive(true)
                    .setMode(PMode.newBuilder()
                        .setOwnerBits(Bits.ALL)
                        .setGroupBits(Bits.ALL)
                        .setOtherBits(Bits.NONE).build())
                    .setWriteType(S3RestUtils.getS3WriteType())
                    .putAllXattr(xattrMap)
                    .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                    .build()
            );
            SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
                .setOwner(user)
                .build();
            mHandler.getMetaFS().setAttribute(new AlluxioURI(
                S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)), attrPOptions);
            if (S3Handler.MULTIPART_CLEANER_ENABLED) {
              MultipartUploadCleaner.apply(mHandler.getMetaFS(), userFs, bucket, object, uploadId);
            }
            return new InitiateMultipartUploadResult(bucket, object, uploadId);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
      });
    }
  } // end of CreateMultipartUploadTask

  /**
   * CompleteMultipartUploadTask.
   */
  public static final class CompleteMultipartUploadTask extends S3ObjectTask {
    private final boolean mKeepAliveEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_ENABLED);
    private final Long mKeepAliveTime = Configuration.getMs(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_TIME_INTERVAL);
    private String mUploadId;
    private FileSystem mUserFs;
    private String mBucket;
    private String mObject;

    /**
     * Create a CompleteMultipartUploadTask.
     * @param handler
     * @param opType
     */
    public CompleteMultipartUploadTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public void handleTaskAsync() {
      try {
        final String user = mHandler.getUser();
        mBucket = mHandler.getBucket();
        mObject = mHandler.getObject();
        final String uploadId = mHandler.getQueryParameter("uploadId");
        LOG.debug("(bucket: {}, object: {}, uploadId: {}) queuing task...",
            mBucket, mObject, uploadId);
        HttpServletResponse httpServletResponse = mHandler.getServletResponse();

        // Set headers before getting committed when flushing whitespaces
        httpServletResponse.setContentType(MediaType.APPLICATION_XML);

        CompletableFuture<Response> respFut = new CompletableFuture<>();
        ProxyWebServer.getInstance().getRequestsExecutor(OpTag.HEAVY).submit(() -> {
          Response completeMpUploadResponse = mHandler.getS3Task().continueTask();
          respFut.complete(completeMpUploadResponse);
        });
        if (mKeepAliveEnabled) {
          // Set status before getting committed when flushing whitespaces
          httpServletResponse.setStatus(HttpServletResponse.SC_OK);
          long sleepMs = 1000;
          while (!respFut.isDone()) {
            LOG.debug("(bucket: {}, object: {}, uploadId: {}) sleeping for {}ms...",
                mBucket, mObject, uploadId, sleepMs);
            try {
              Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
              LOG.error(e.toString());
            }
            // TODO(czhu): figure out how to send whitespace characters while still
            // returning a correct status code
            // - calling getWriter().flush() commits the response (headers, status code, etc.)
            // - https://docs.oracle.com/javaee/7/api/javax/servlet/ServletResponse.html#getWriter--
            // periodically sends white space characters to keep the connection from timing out
            LOG.debug("(bucket: {}, object: {}, uploadId: {}) sending whitespace...",
                mBucket, mObject, uploadId);
            httpServletResponse.getWriter().print(" ");
            httpServletResponse.getWriter().flush();
            sleepMs = Math.min(2 * sleepMs, mKeepAliveTime);
          }
        } // otherwise we perform a blocking call on future.get()

        XmlMapper mapper = new XmlMapper();
        try {
          Response result = respFut.get();
          if (!mKeepAliveEnabled) {
            S3Handler.processResponse(httpServletResponse, result);
          } else {
            // entity is already a String from a serialized CompleteMultipartUploadResult
            String entityStr = result.getEntity().toString();
            httpServletResponse.getWriter().write(entityStr);
          }
        } catch (Exception e) {
          Throwable cause = e.getCause();
          if (cause instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) cause;
            httpServletResponse.getWriter().write(mapper.writeValueAsString(
                new CompleteMultipartUploadResult(s3Exception.getErrorCode().getCode(),
                    s3Exception.getErrorCode().getDescription())));
            if (!mKeepAliveEnabled) {
              httpServletResponse.setStatus(s3Exception.getErrorCode().getStatus().getStatusCode());
            }
          }
          LOG.error(ThreadUtils.formatStackTrace(cause));
        }
      } catch (Exception e) {
        // This try-catch is not intended to handle any exceptions, it is purely
        // to ensure that encountered exceptions get logged.
        LOG.error("Unhandled exception for {}/{}. {}", mHandler.getBucket(),
            mHandler.getObject(), ThreadUtils.formatStackTrace(e));
//                throw e;
      }
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // CompleteMultipartUploadTask ...
        String objectPath = null;
        String objTempPath = null;
        mUploadId = mHandler.getQueryParameter("uploadId");
        final String bucket = mHandler.getBucket();
        final String object = mHandler.getObject();
        final String user = mHandler.getUser();
        mUserFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        try {
          String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
          S3RestUtils.checkPathIsAlluxioDirectory(mUserFs, bucketPath, null);
          objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
          // Check for existing multipart info files and dirs
          AlluxioURI multipartTemporaryDir = new AlluxioURI(
              S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, mUploadId));
          URIStatus metaStatus;

          try (com.codahale.metrics.Timer.Context ctx = MetricsSystem
              .uniformTimer(MetricKey.PROXY_CHECK_UPLOADID_STATUS_LATENCY.getName()).time()) {
            metaStatus = S3RestUtils.checkStatusesForUploadId(mHandler.getMetaFS(), mUserFs,
                multipartTemporaryDir, mUploadId).get(1);
          } catch (Exception e) {
            LOG.warn("checkStatusesForUploadId uploadId:{} failed. {}", object,
                ThreadUtils.formatStackTrace(e));
            throw new S3Exception(objectPath, S3ErrorCode.NO_SUCH_UPLOAD);
          }

          // Parse the HTTP request body to get the intended list of parts
          CompleteMultipartUploadRequest request = parseCompleteMultipartUploadRequest(objectPath);

          // Check if the requested parts are available
          List<URIStatus> uploadedParts = validateParts(request, objectPath, multipartTemporaryDir);

          // (re)create the merged object to a temporary object path
          LOG.debug("CompleteMultipartUploadTask (bucket: {}, object: {}, uploadId: {}) "
              + "combining {} parts...", bucket, object, mUploadId, uploadedParts.size());
          CreateFilePOptions createFileOption = prepareForCreateTempFile(metaStatus);
          objTempPath = objectPath + ".temp." + UUID.randomUUID();
          AlluxioURI objectTempUri = new AlluxioURI(objTempPath);
          FileOutStream os = mUserFs.createFile(objectTempUri, createFileOption);
          MessageDigest md5 = MessageDigest.getInstance("MD5");

          try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5);
               com.codahale.metrics.Timer.Context ctx = MetricsSystem
                   .uniformTimer(MetricKey.PROXY_COMPLETE_MP_UPLOAD_MERGE_LATENCY
                       .getName()).time()) {
            for (URIStatus part : uploadedParts) {
              try (FileInStream is = mUserFs.openFile(new AlluxioURI(part.getPath()))) {
                ByteStreams.copy(is, digestOutputStream);
              }
            }
          }
          // persist the ETag via xAttr
          String entityTag = Hex.encodeHexString(md5.digest());
          // TODO(czhu): try to compute the ETag prior to creating the file to reduce total RPC RTT
          S3RestUtils.setEntityTag(mUserFs, objectTempUri, entityTag);
          // rename the temp file to the target object file path
          AlluxioURI objectUri = new AlluxioURI(objectPath);
          mUserFs.rename(objectTempUri, objectUri, RenamePOptions.newBuilder()
              .setPersist(WriteType.fromProto(createFileOption.getWriteType()).isThrough())
              .setS3SyntaxOptions(S3SyntaxOptions.newBuilder()
                  .setOverwrite(true)
                  .setIsMultipartUpload(true)
                  .build())
              .build());

          // Remove the temporary directory containing the uploaded parts and the
          // corresponding Alluxio S3 API metadata file
          try (Timer.Context ctx = MetricsSystem
              .uniformTimer(MetricKey.PROXY_CLEANUP_MULTIPART_UPLOAD_LATENCY.getName()).time()) {
            removePartsDirAndMPMetaFile(multipartTemporaryDir);
          }
          return new CompleteMultipartUploadResult(objectPath, bucket, object, entityTag);
        } catch (Exception e) {
        /* On exception we always check if someone completes the multipart object before us to
        achieve idempotency: when a race caused by retry(most cases), the commit of
        this object happens at time of rename op, check DefaultFileSystemMaster.rename.
         * */
          LOG.warn("Exception during CompleteMultipartUpload:{}", ThreadUtils.formatStackTrace(e));
          if (objectPath != null) {
            URIStatus objStatus = checkIfComplete(objectPath);
            if (objStatus != null) {
              String etag = new String(objStatus.getXAttr()
                  .getOrDefault(S3Constants.ETAG_XATTR_KEY, new byte[0]));
              if (!etag.isEmpty()) {
                LOG.info("Check for idempotency, uploadId:{} idempotency check passed.", mUploadId);
                return new CompleteMultipartUploadResult(objectPath, bucket, object, etag);
              }
              LOG.info("Check for idempotency, uploadId:{} object path exists but no etag found.",
                  mUploadId);
            }
          }
          throw S3RestUtils.toObjectS3Exception(e, object);
        } finally {
          // Cleanup temp obj path no matter what, if path not exist, ignore
          cleanupTempPath(objTempPath);
        }
      });
    }

    /**
     * Prepare CreateFilePOptions for create temp multipart upload file.
     *
     * @param metaStatus multi part upload meta file status
     * @return CreateFilePOptions
     */
    public CreateFilePOptions prepareForCreateTempFile(URIStatus metaStatus) {
      CreateFilePOptions.Builder optionsBuilder = CreateFilePOptions.newBuilder()
          .setRecursive(true)
          .setMode(PMode.newBuilder()
              .setOwnerBits(Bits.ALL)
              .setGroupBits(Bits.ALL)
              .setOtherBits(Bits.NONE).build())
          .putXattr(PropertyKey.Name.S3_UPLOADS_ID_XATTR_KEY,
              ByteString.copyFrom(mUploadId, StandardCharsets.UTF_8))
          .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
          .setWriteType(S3RestUtils.getS3WriteType());
      // Copy Tagging xAttr if it exists
      if (metaStatus.getXAttr().containsKey(S3Constants.TAGGING_XATTR_KEY)) {
        optionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
            ByteString.copyFrom(metaStatus.getXAttr().get(S3Constants.TAGGING_XATTR_KEY)));
      }
      // Copy Content-Type Header xAttr if it exists
      if (metaStatus.getXAttr().containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
        optionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
            ByteString.copyFrom(metaStatus.getXAttr().get(S3Constants.CONTENT_TYPE_XATTR_KEY)));
      }
      return optionsBuilder.build();
    }

    /**
     * Parse xml http body for CompleteMultipartUploadRequest.
     *
     * @param objectPath
     * @return CompleteMultipartUploadRequest
     * @throws S3Exception
     */
    public CompleteMultipartUploadRequest parseCompleteMultipartUploadRequest(String objectPath)
        throws S3Exception {
      CompleteMultipartUploadRequest request;
      try {
        request = new XmlMapper().readerFor(CompleteMultipartUploadRequest.class)
            .readValue(mHandler.getInputStream());
      } catch (IllegalArgumentException | IOException e) {
        LOG.error("Failed parsing CompleteMultipartUploadRequest:{}",
            ThreadUtils.formatStackTrace(e));
        Throwable cause = e.getCause();
        if (cause instanceof S3Exception) {
          throw S3RestUtils.toObjectS3Exception((S3Exception) cause, objectPath);
        }
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
      return request;
    }

    /**
     * Validate the parts as part of this multipart uplaod request.
     *
     * @param request
     * @param objectPath
     * @param multipartTemporaryDir
     * @return List of status of the part files
     * @throws S3Exception
     * @throws IOException
     * @throws AlluxioException
     */
    public List<URIStatus> validateParts(CompleteMultipartUploadRequest request,
                                         String objectPath,
                                         AlluxioURI multipartTemporaryDir)
        throws S3Exception, IOException, AlluxioException {
      List<URIStatus> uploadedParts = mUserFs.listStatus(multipartTemporaryDir);
      uploadedParts.sort(new S3RestUtils.URIStatusNameComparator());
      if (uploadedParts.size() < request.getParts().size()) {
        throw new S3Exception(objectPath, S3ErrorCode.INVALID_PART);
      }
      Map<Integer, URIStatus> uploadedPartsMap = uploadedParts.stream().collect(Collectors.toMap(
          status -> Integer.parseInt(status.getName()),
          status -> status
      ));
      int lastPartNum = request.getParts().get(request.getParts().size() - 1).getPartNumber();
      for (CompleteMultipartUploadRequest.Part part : request.getParts()) {
        if (!uploadedPartsMap.containsKey(part.getPartNumber())) {
          throw new S3Exception(objectPath, S3ErrorCode.INVALID_PART);
        }
        if (part.getPartNumber() != lastPartNum // size requirement not applicable to last part
            && uploadedPartsMap.get(part.getPartNumber()).getLength() < Configuration.getBytes(
            PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_MIN_PART_SIZE)) {
          throw new S3Exception(objectPath, S3ErrorCode.ENTITY_TOO_SMALL);
        }
      }
      return uploadedParts;
    }

    /**
     * Cleanup the multipart upload temporary folder holding the parts files.
     * and the meta file for this multipart.
     *
     * @param multipartTemporaryDir
     * @throws IOException
     * @throws AlluxioException
     */
    public void removePartsDirAndMPMetaFile(AlluxioURI multipartTemporaryDir)
        throws IOException, AlluxioException {
      mUserFs.delete(multipartTemporaryDir,
          DeletePOptions.newBuilder().setRecursive(true).build());
      mHandler.getMetaFS().delete(new AlluxioURI(
              S3RestUtils.getMultipartMetaFilepathForUploadId(mUploadId)),
          DeletePOptions.newBuilder().build());
      if (S3Handler.MULTIPART_CLEANER_ENABLED) {
        MultipartUploadCleaner.cancelAbort(mHandler.getMetaFS(), mUserFs,
            mBucket, mObject, mUploadId);
      }
    }

    /**
     * Cleanup the temp object file for complete multipart upload.
     *
     * @param objTempPath
     */
    public void cleanupTempPath(String objTempPath) {
      if (objTempPath != null) {
        try (Timer.Context ctx = MetricsSystem
            .uniformTimer(MetricKey.PROXY_CLEANUP_TEMP_MULTIPART_UPLOAD_OBJ_LATENCY
                .getName()).time()) {
          mUserFs.delete(new AlluxioURI(objTempPath), DeletePOptions.newBuilder().build());
        } catch (Exception e) {
          LOG.warn("Failed to clean up temp path:{}, {}", objTempPath, e.getMessage());
        }
      }
    }

    /**
     * On any exception, check with Master on if the there's an object file.
     * bearing the same upload id already got completed.
     *
     * @param objectPath
     * @return the status of the existing object through CompleteMultipartUpload call
     */
    public URIStatus checkIfComplete(String objectPath) {
      try {
        URIStatus objStatus = mUserFs.getStatus(new AlluxioURI(objectPath));
        String uploadId = new String(objStatus.getXAttr()
            .getOrDefault(PropertyKey.Name.S3_UPLOADS_ID_XATTR_KEY, new byte[0]));
        if (objStatus.isCompleted() && StringUtils.equals(uploadId, mUploadId)) {
          return objStatus;
        }
      } catch (IOException | AlluxioException ex) {
        // can't validate if any previous attempt has succeeded
        LOG.warn("Check for objectPath:{} failed:{}, unsure if the complete status.",
            objectPath, ex.getMessage());
        return null;
      }
      return null;
    }
  } // end of CompleteMultipartUploadTask

  private static final class AbortMultipartUploadTask extends S3ObjectTask {

    public AbortMultipartUploadTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // AbortMultipartUploadTask ...
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(
            user, mHandler.getMetaFS());
        final String uploadId = mHandler.getQueryParameter("uploadId");
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        AlluxioURI multipartTemporaryDir = new AlluxioURI(S3RestUtils
            .getMultipartTemporaryDirForObject(bucketPath, mHandler.getObject(), uploadId));
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            "abortMultipartUpload", user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          try {
            S3RestUtils.checkStatusesForUploadId(mHandler.getMetaFS(),
                userFs, multipartTemporaryDir, uploadId);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception((e instanceof FileDoesNotExistException)
                    ? new S3Exception(mHandler.getObject(), S3ErrorCode.NO_SUCH_UPLOAD) : e,
                mHandler.getObject(), auditContext);
          }

          try {
            userFs.delete(multipartTemporaryDir,
                DeletePOptions.newBuilder().setRecursive(true).build());
            mHandler.getMetaFS().delete(new AlluxioURI(
                    S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)),
                DeletePOptions.newBuilder().build());
            if (S3Handler.MULTIPART_CLEANER_ENABLED) {
              MultipartUploadCleaner.cancelAbort(mHandler.getMetaFS(), userFs,
                  mHandler.getBucket(), mHandler.getObject(), uploadId);
            }
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }

        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return Response.Status.NO_CONTENT;
      });
    }
  } // end of AbortMultipartUploadTask

  private static final class DeleteObjectTaggingTask extends S3ObjectTask {

    public DeleteObjectTaggingTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // DeleteObjectTaggingTask ...
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        LOG.debug("DeleteObjectTagging object={}", mHandler.getObject());
        Map<String, ByteString> xattrMap = new HashMap<>();
        xattrMap.put(S3Constants.TAGGING_XATTR_KEY, ByteString.copyFrom(new byte[0]));
        SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
            .putAllXattr(xattrMap).setXattrUpdateStrategy(File.XAttrUpdateStrategy.DELETE_KEYS)
            .build();
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            "deleteObjectTags", user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          try {
            userFs.setAttribute(new AlluxioURI(objectPath), attrPOptions);
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return Response.Status.NO_CONTENT;
      });
    }
  } // end of DeleteObjectTaggingTask

  private static final class DeleteObjectTask extends S3ObjectTask {

    public DeleteObjectTask(S3Handler handler, OpType opType) {
      super(handler, opType);
    }

    @Override
    public Response continueTask() {
      return S3RestUtils.call(getObjectTaskResource(), () -> {
        // DeleteObjectTask ...
        Preconditions.checkNotNull(mHandler.getBucket(), "required 'bucket' parameter is missing");
        Preconditions.checkNotNull(mHandler.getObject(), "required 'object' parameter is missing");

        final String user = mHandler.getUser();
        final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
        // Delete the object.
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
        DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(Configuration
                .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
            .build();
        try (S3AuditContext auditContext = mHandler.createAuditContext(
            "deleteObject", user, mHandler.getBucket(), mHandler.getObject())) {
          S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
          try {
            userFs.delete(new AlluxioURI(objectPath), options);
          } catch (FileDoesNotExistException | DirectoryNotEmptyException e) {
            // intentionally do nothing, this is ok. It should result in a 204 error
            // This is the same response behavior as AWS's S3.
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath, auditContext);
          }
        }
        // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
        return Response.Status.NO_CONTENT;
      });
    }
  } // end of DeleteObjectTask
}
