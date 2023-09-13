package alluxio.underfs;

import alluxio.collections.Pair;
import alluxio.exception.status.CanceledException;
import alluxio.underfs.options.MultipartUfsOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectMultipartUploader implements MultipartUploader {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectMultipartUploader.class);

  private final String mPath;
  private String mUploadId;
  private String mContentHash;
  private final Map<Integer, UploadTask> mUploadTasks = new TreeMap<>();
  private final Map<Integer, Pair<Integer, String>> mETags = new HashMap<>();
  private final ObjectUnderFileSystem mUfs;
  private final ListeningExecutorService mExecutor;

  public String getContentHash() {
    return mContentHash;
  }

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
  public ListenableFuture<Void> putPart(InputStream in, int partNumber, long partSize) throws IOException {
    ListenableFuture<Void> f = mExecutor.submit(() -> {
      try {
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
      //TODO (wyy) get content hash
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
    mContentHash = mUfs.completeMultiPart(mPath, mUploadId, etags, MultipartUfsOptions.defaultOption());
  }

  @Override
  public void abort() throws IOException {
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

  // TODO (wyy) to record the UploadTask in memory
  private class UploadTask{
    private int mPartNumber;
    private long mSize;
    private ListenableFuture<Void> mFuture;

    public UploadTask(int partNumber, long size, ListenableFuture<Void> future) {
      mPartNumber = partNumber;
      mSize = size;
      mFuture = future;

    }

    public UploadTask(int partNumber, Path path, long size, ListenableFuture<Void> future) {
      mPartNumber = partNumber;
      mSize = size;
      mFuture = future;
    }

    public boolean finished() {
      try {
        mFuture.get();
        return true;
      } catch (Exception e) {
        //log
        return false;
      }
    }
  }
}
