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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.DownloadFileRequest;
import com.aliyun.oss.internal.OSSConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

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
    private final OSSClient mOssClient;

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
    OSSLowLevelInputStream(String bucketName, String key, OSSClient client, long position,
                   RetryPolicy retryPolicy, long multiRangeChunkSize,
                   long streamingDownloadPartitionSize, int taskNum, List<String> tmpDirs) throws IOException {
        super(multiRangeChunkSize);
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
        DownloadFileRequest req = new DownloadFileRequest(mBucketName, mKey);
        // Sets the concurrent task thread count 5. By default it's 1.
        req.setTaskNum(mTaskNum);
        // Sets the part size, by default it's 1K.
        req.setPartSize(1024 * mStreamingDownloadPartitionSize);
        // Enable checkpoint.
        req.setEnableCheckpoint(true);

        // Make the temp file
        File tmpFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(mTmpDirs), mKey));
        tmpFile.getParentFile().mkdirs();

        OSSException lastException = null;
        LOG.debug("Create stream for key {} in bucket {} from {} to {}",
                mKey, mBucketName, startPos, endPos);
        while (mRetryPolicy.attempt()) {
            try {
                mOssClient.downloadFile(req);
                fis = new FileInputSteam(tmpFile);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();

                if (startPos > 0){
                    fis.skip(startPos);
                }

                long length = endPos < mContentLength ? endPos - 1 : mContentLength - 1;
                byte[] buffer = new byte[OSSConstants.DEFAULT_BUFFER_SIZE];
                int bytesRead;
                // all the bytes read from scratch
                long bytesReadInAll = 0;
                while ((bytesRead = fis.read(buf)) != -1 ) {
                    bytesReadInAll += (long)bytesRead;
                    if (bytesReadInAll > length) {
                        // If the bytes read is more than the length (endPos - startPos), only read the length
                        bytesRead = (int)(bytesReadInAll - length);
                    }else if (bytesReadInAll == length){
                        break;
                    }
                    bos.write(buffer, 0, bytesRead);
                }

                bos.flush();
                //    byte[] bytes = bos.toByteArray();
                return new BufferedInputStream(bos);
            } catch (OSSException e) {
                LOG.warn("Attempt {} to open key {} in bucket {} failed with exception : {}",
                        mRetryPolicy.getAttemptCount(), mKey, mBucketName, e.toString());
                if (!e.getErrorCode().equals("NoSuchKey")) {
                    throw new IOException(e);
                }
                // Key does not exist
                lastException = e;
            } finally {
                // Delete the temporary downloaded file
                if (!tmpFile.delete()) {
                    LOG.error("Failed to delete temporary file @ {}", tmpFile.getPath());
                }
            }
        }
        // Failed after retrying key does not exist
        throw new IOException(lastException);
    }

}
