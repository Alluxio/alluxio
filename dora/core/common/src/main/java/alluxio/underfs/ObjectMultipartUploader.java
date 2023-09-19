package alluxio.underfs;

import alluxio.collections.Pair;
import alluxio.underfs.options.MultipartUfsOptions;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The multipart uploader implementation for object-store UFS to support multipart uploading.
 */
public class ObjectMultipartUploader implements MultipartUploader {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectMultipartUploader.class);

  private final String mPath;
  private String mUploadId;
  private String mContentHash;
  private final Map<Integer, UploadTask> mUploadTasks = new TreeMap<>();
  private final Map<Integer, Pair<Integer, String>> mETags = new HashMap<>();
  private final ObjectUnderFileSystem mUfs;
  private final ListeningExecutorService mExecutor;

  /**
   * Gets the content hash of the final completed file.
   * @return content hash
   */
  public String getContentHash() {
    return mContentHash;
  }

  /**
   * Constructs an instance of {@link ObjectMultipartUploader}.
   * @param path
   * @param ufs
   * @param executor
   */
  public ObjectMultipartUploader(String path, ObjectUnderFileSystem ufs,
                                 ListeningExecutorService executor) {
    mPath = path;
    mUfs = ufs;
    mExecutor = executor;
  }

  @Override
  public void startUpload() throws IOException {
    mUploadId = mUfs.initMultiPart(mPath, MultipartUfsOptions.defaultOption());
  }

  @Override
  public ListenableFuture<Void> putPart(InputStream in, int partNumber, long partSize)
      throws IOException {
    ListenableFuture<Void> f = mExecutor.submit(() -> {
      try {
        LOG.warn("put part for id {}, partnumber is {}.", mUploadId, partNumber);
        String etag = mUfs.uploadPartWithStream(mPath, mUploadId, partNumber, partSize, in,
            MultipartUfsOptions.defaultOption());
        Pair<Integer, String> etagPair = new Pair<>(partNumber, etag);
        mETags.put(partNumber, etagPair);
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    mUploadTasks.put(partNumber, new UploadTask(partNumber, partSize, f));
    return f;
  }

  @Override
  public ListenableFuture<Void> putPart(ByteBuffer b, int partNumber) throws IOException {
    long partSize = b.remaining();
    ListenableFuture<Void> f = mExecutor.submit(() -> {
      try {
        LOG.warn("put part for id {}, partnumber is {}.", mUploadId, partNumber);
        InputStream in = new ByteBufferInputStream(b);
        String etag = mUfs.uploadPartWithStream(mPath, mUploadId, partNumber, partSize, in,
            MultipartUfsOptions.defaultOption());
        Pair<Integer, String> etagPair = new Pair<>(partNumber, etag);
        mETags.put(partNumber, etagPair);
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    mUploadTasks.put(partNumber, new UploadTask(partNumber, partSize, f));
    return f;
  }

  @Override
  public void complete() throws IOException {
    // If there is no upload task, it means the file is empty.
    // Just create an empty file and set up the permissions in this case.
    if (mUploadTasks.isEmpty()) {
      mUfs.createEmptyObject(mPath);
      //TODO(wyy) get content hash
      LOG.debug("Object store ufs multipart upload finished for {}", mPath);
      return;
    }

    flush();
    // complete
    List<Pair<Integer, String>> etags = new ArrayList<>();
    for (Map.Entry<Integer, UploadTask> entry : mUploadTasks.entrySet()) {
      UploadTask uploadTask = entry.getValue();
      if (uploadTask.finished()) {
        Integer partNum = entry.getKey();
        etags.add(mETags.get(partNum));
      }
    }
    LOG.warn("complete mpu for id {}, mETags is {}.", mUploadId, etags);
    mContentHash =
        mUfs.completeMultiPart(mPath, mUploadId, etags, MultipartUfsOptions.defaultOption());
    LOG.warn("complete mpu success for id {}, mETags is {}.", mUploadId, etags);
  }

  @Override
  public void abort() throws IOException {
    LOG.warn("abort mpu for id {}.", mUploadId);
    mUfs.abortMultipartTask(mPath, mUploadId, MultipartUfsOptions.defaultOption());
  }

  @Override
  public void flush() throws IOException {
    if (mUploadTasks.isEmpty()) {
      return;
    }
    Set<ListenableFuture<Void>> futures =
        mUploadTasks.values().stream().map(it -> it.mFuture).collect(Collectors.toSet());
    try {
      Futures.allAsList(futures).get();
    } catch (ExecutionException e) {
      // No recover ways so that we need to cancel all the upload tasks
      // and abort the multipart upload
      Futures.allAsList(futures).cancel(true);
      abort();
      throw new IOException(
          "Part upload failed in multipart upload with to " + mPath, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted object upload.", e);
      Futures.allAsList(futures).cancel(true);
      abort();
      Thread.currentThread().interrupt();
    }
  }

  private class UploadTask {
    private int mPartNumber;
    private long mSize;
    private ListenableFuture<Void> mFuture;

    public UploadTask(int partNumber, long size, ListenableFuture<Void> future) {
      mPartNumber = partNumber;
      mSize = size;
      mFuture = future;
    }

    public boolean finished() {
      try {
        mFuture.get();
        return true;
      } catch (Exception e) {
        LOG.warn("error in get result in uploading part:", e);
        return false;
      }
    }
  }
}
