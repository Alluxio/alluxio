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
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.zookeeper.server.ByteBufferInputStream;

public class ObjectMultipartUploader implements MultipartUploader {
  private final String mPath;
  private String mUploadId;
  private final Map<Integer, UploadTask> mUploadTasks = new TreeMap<>();
  private final Map<Integer, Pair<Integer, String>> mETags = new HashMap<>();
  private final ObjectUnderFileSystem mUfs;
  private final ListeningExecutorService mExecutor;

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
        mUfs.uploadPartWithStream(mPath, mUploadId, partNumber, partSize, in,
            MultipartUfsOptions.defaultOption());
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    mUploadTasks.put(partNumber, new UploadTask());
    return f;
  }

  @Override
  public ListenableFuture<Void> putPart(ByteBuffer b, int partNumber) throws IOException {
    ListenableFuture<Void> f = mExecutor.submit(() -> {
      try {
        long partSize = b.remaining();
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
    mUploadTasks.put(partNumber, new UploadTask());
    return f;
  }

  @Override
  public void complete() throws IOException {
    if (mUploadTasks.isEmpty()) {
      // create empty file?
    }

    // flush

    // complete
    List<Pair<Integer, String>> etags = new ArrayList<>();
    for (Map.Entry<Integer, UploadTask> entry : mUploadTasks.entrySet()) {
      UploadTask uploadTask = entry.getValue();
      if (uploadTask.finished()) {
        Integer partNum = entry.getKey();
        etags.add(mETags.get(partNum));
      }
    }
    mUfs.completeMultiPart(mPath, mUploadId, etags, MultipartUfsOptions.defaultOption());
  }

  @Override
  public void abort() throws IOException {
    mUfs.abortMultipartTask(mPath, mUploadId, MultipartUfsOptions.defaultOption());
  }

  @Override
  public void flush() throws IOException {
    doFlush();
  }


  private void doFlush() throws IOException {
    try {
      Futures.allAsList(
          mUploadTasks.values().stream().map(it -> it.mFuture).collect(Collectors.toSet())).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  private class UploadTask{
    private int mPartNumber;
    private int mSize;
    private ListenableFuture<Void> mFuture;

    public UploadTask() {
    }

    public UploadTask(int partNumber, Path path, int size, ListenableFuture<Void> future) {
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
