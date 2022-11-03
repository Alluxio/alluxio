package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.proto.journal.File;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

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

    public static S3ObjectTask allocateTask(S3Handler handler) {
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
                if (handler.getQueryParameter("uploadId") != null) {
                    action = ACTION.UploadPart;
                    if (StringUtils.isNotEmpty(task.amzHeaderMap_.get(S3Constants.S3_COPY_SOURCE_HEADER)))
                        action = ACTION.UploadPartCopy;
                } else if (handler.getQueryParameter("tagging") != null) {
                    return new PutObjectTaggingTask(handler, OpType.PutObjectTagging);
                } else {
                    action = ACTION.PutObject;
                    if (StringUtils.isNotEmpty(task.amzHeaderMap_.get(S3Constants.S3_COPY_SOURCE_HEADER)))
                        action = ACTION.CopyObject;
                }
                break;
            case "POST":
                if (handler.getQueryParameter("uploads") != null) {
                    action = ACTION.CreateMultipartUpload;
                }
                break;
            case "HEAD":
                return new HeadObjectTask(handler, OpType.HeadObject);
                break;
            case "DELETE":
//                if (handler.getQueryParameter("uploadId") != null) {
//                    action = ACTION.AbortMultipartUpload;
//                } else if (handler.getQueryParameter("tagging") != null) {
//                    action = ACTION.DeleteObjectTagging;
//                } else {
//                    action = ACTION.DeleteObject;
//                }
//                break;
            default:
                return new S3ObjectTask(handler, OpType.Unsupported);
        }
    }

    public String getObjectTaskResource() {
        return mHandler.getBucket() + AlluxioURI.SEPARATOR + mHandler.getObject();
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
                        this.mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
                    S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);

                    AlluxioURI tmpDir = new AlluxioURI(
                            S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, mHandler.getObject(), uploadId));
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
                        this.mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
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

        protected PutObjectTaggingTask(S3Handler handler, OpType opType) {
            super(handler, opType);
        }

        @Override
        public Response continueTask() {
            return S3RestUtils.call(getObjectTaskResource(), () -> {
                final String user = mHandler.getUser();
                final FileSystem userFs = S3RestUtils.createFileSystemForUser(user, mHandler.getMetaFS());
                String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mHandler.getBucket());
                try (S3AuditContext auditContext = mHandler.createAuditContext(
                        this.mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
                    S3RestUtils.checkPathIsAlluxioDirectory(userFs, bucketPath, auditContext);
                    String objectPath = bucketPath + AlluxioURI.SEPARATOR + mHandler.getObject();
                    AlluxioURI objectUri = new AlluxioURI(objectPath);
                    TaggingData tagData = null;
                    try {
                        tagData = new XmlMapper().readerFor(TaggingData.class).readValue(mHandler.getInputStream());
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
                        this.mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
                    try {
                        URIStatus status = userFs.getStatus(objectUri);
                        FileInStream is = userFs.openFile(objectUri);
                        S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
                        RangeFileInStream ris = RangeFileInStream.Factory.create(is, status.getLength(), s3Range);

                        Response.ResponseBuilder res = Response.ok(ris, MediaType.APPLICATION_OCTET_STREAM_TYPE)
                                .lastModified(new Date(status.getLastModificationTimeMs()))
                                .header(S3Constants.S3_CONTENT_LENGTH_HEADER, s3Range.getLength(status.getLength()));

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
                        this.mOPType.name(), user, mHandler.getBucket(), mHandler.getObject())) {
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
}
