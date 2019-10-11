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

package alluxio.underfs.oss;

import alluxio.retry.RetryPolicy;
import alluxio.underfs.MultiRangeObjectInputStream;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DownloadFileRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.util.List;

/**
 * A stream for reading a file from OSS. This input stream in multiple stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class OSSLowLevelInputStream extends MultiRangeObjectInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(OSSLowLevelInputStream.class);

    /** Bucket name of the Alluxio OSS bucket. */
    private final String mBucketName;

    /** Key of the file in OSS to read. */
    private final String mKey;

    /** The OSS client for OSS operations. */
    private final OSS mOssClient;

    /** The size of the object in bytes. */
    private final long mContentLength;

    /** The maximum allowed size of a partition. */
    private final long mStreamingDownloadPartitionSize;

    /** The task count for downloading parts */
    private final int mTaskNum;

    private final List<String> mTmpDirs;

    /**
     * Policy determining the retry behavior in case the key does not exist. The key may not exist
     * because of eventual consistency.
     */
    private final RetryPolicy mRetryPolicy;

    /**
     * Creates a new instance of {@link MultiRangeObjectInputStream}.
     *
     * @param bucketName the name of the bucket
     * @param key the key of the file
     * @param client the client for OSS
     * @param position the position to begin reading from
     * @param retryPolicy retry policy in case the key does not exist
     * @param multiRangeChunkSize the chunk size to use on this stream
     * @param streamingDownloadPartitionSize the size in bytes for partitions of streaming downloads
     * @param taskNum task count for downloading parts
     * @param tmpDirs a list of temporary directories
     */
    OSSLowLevelInputStream(String bucketName, String key, OSS client, long position,
                   RetryPolicy retryPolicy, long multiRangeChunkSize,
                   long streamingDownloadPartitionSize, int taskNum, List<String> tmpDirs) throws IOException {
        super(multiRangeChunkSize);
        LOG.debug("Use OSSLowLevelInputStream.");
        mBucketName = bucketName;
        mKey = key;
        mOssClient = client;
        mPos = position;
        ObjectMetadata meta = mOssClient.getObjectMetadata(mBucketName, key);
        mContentLength = meta == null ? 0 : meta.getContentLength();
        mRetryPolicy = retryPolicy;
        mStreamingDownloadPartitionSize = streamingDownloadPartitionSize;
        mTaskNum = taskNum;
        mTmpDirs = tmpDirs;
    }

    @Override
    protected InputStream createStream(long startPos, long endPos)
            throws IOException {
        if (endPos - startPos > mStreamingDownloadPartitionSize) {
            int taskNum = (int)((endPos-startPos) / mStreamingDownloadPartitionSize);
            if (taskNum > mTaskNum) {
                taskNum = mTaskNum;
            }
            return createStreamWithPartition(startPos, endPos, taskNum);
        }else{
            return createStreamWithoutPartition(startPos, endPos);
        }
    }

    /**
     * Opens a new stream without multiple partitions reading a range. When endPos > content length, the returned stream should
     * read till the last valid byte of the input. The behaviour is undefined when (startPos < 0),
     * (startPos >= content length), or (endPos <= 0).
     *
     * @param startPos start position in bytes (inclusive)
     * @param endPos end position in bytes (exclusive)
     * @return a new {@link InputStream}
     */
    private InputStream createStreamWithoutPartition(long startPos, long endPos)
            throws IOException {
        GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);
        // OSS returns entire object if we read past the end
        req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
        OSSException lastException = null;
        LOG.debug("Create stream without partition for key {} in bucket {} from {} to {}",
                mKey, mBucketName, startPos, endPos);
        while (mRetryPolicy.attempt()) {
            try {
                OSSObject ossObject = mOssClient.getObject(req);
                return new BufferedInputStream(ossObject.getObjectContent());
            } catch (OSSException e) {
                LOG.warn("Attempt {} to open key {} in bucket {} failed with exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                LOG.warn("IOException " + Throwables.getStackTraceAsString(e));
                if (!e.getErrorCode().equals("NoSuchKey")) {
                    throw new IOException(e);
                }
                // Key does not exist
                lastException = e;
            }
        }
        // Failed after retrying key does not exist
        throw new IOException(lastException);
    }

    /**
     * Opens a new stream with multiple partitions reading a range. When endPos > content length, the returned stream should
     * read till the last valid byte of the input. The behaviour is undefined when (startPos < 0),
     * (startPos >= content length), or (endPos <= 0).
     *
     * @param startPos start position in bytes (inclusive)
     * @param endPos end position in bytes (exclusive)
     * @param taskNum the task number to download the file
     * @return a new {@link InputStream}
     */
    private InputStream createStreamWithPartition(long startPos, long endPos, int taskNum)
            throws IOException {
        LOG.debug("the taskNum: {} and the partition size is {}", taskNum, mStreamingDownloadPartitionSize);
        DownloadFileRequest req = new DownloadFileRequest(mBucketName, mKey);
        // Sets the concurrent task thread count 5. By default it's 1.
        req.setTaskNum(taskNum);
        // Sets the part size, by default it's 1K.
        req.setPartSize(mStreamingDownloadPartitionSize);
        // Enable checkpoint.
        req.setEnableCheckpoint(false);

        // Create the temp file
        File tmpFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(mTmpDirs), mKey));
        boolean created = tmpFile.getParentFile().mkdirs();
        LOG.debug("the tmp directory is created: "+ created);
        // Set the download file.
        req.setDownloadFile(tmpFile.getAbsolutePath());

        OSSException lastException = null;
        long start = 0;
        long length;
        FileInputStream fis = null;
        LOG.debug("Create stream with partition for key {} in bucket {} from {} to {}",
                mKey, mBucketName, startPos, endPos);
        while (mRetryPolicy.attempt()) {
            try {
                // measure the performance of oss
                if (LOG.isDebugEnabled()) {
                    start = System.currentTimeMillis();
                }
                mOssClient.downloadFile(req);
                LOG.debug("Calling OSS download file method took: {} ms", (System.currentTimeMillis()-start));
                fis = new FileInputStream(tmpFile);

                if (startPos > 0){
                    fis.skip(startPos);
                }

                length = endPos - startPos < mContentLength ? endPos - startPos : mContentLength ;
                LOG.debug("The mContentLength is {}, endPos - startPos is {}, the length is {}",
                        mContentLength, endPos - startPos, length);
                byte[] bytes = IOUtils.toByteArray(fis, length);
                return new BufferedInputStream(new ByteArrayInputStream(bytes));
            } catch (OSSException e) {
                LOG.warn("Attempt {} to open key {} in bucket {} failed with OSS exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                // Key does not exist
                if (!e.getErrorCode().equals("NoSuchKey")) {
                    throw new IOException(e);
                }
                lastException = e;
            } catch (IOException e) {
                LOG.warn("Attempt {} to open key {} in bucket {} failed with IO exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                LOG.warn("IOException: " + Throwables.getStackTraceAsString(e));
                lastException = new OSSException(e.getMessage(), e);
            } catch (Throwable e) {
                LOG.warn("Failed {} to open key {} in bucket {} failed with exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                LOG.warn("Throwable: " + Throwables.getStackTraceAsString(e));
                throw new IOException(e);
            } finally {
                LOG.debug("Calling createStreamWithPartition took: {} ms", (System.currentTimeMillis()-start));
                if (fis != null) {
                    fis.close();
                }
                // Delete the temporary downloaded file
                if (tmpFile.exists()) {
                    if (!tmpFile.delete()) {
                        LOG.warn("Failed to delete temporary file @ {}", tmpFile.getAbsolutePath());
                    }
                }
            }
        }
        // Failed after retrying key does not exist
        throw new IOException(lastException);
    }
}
