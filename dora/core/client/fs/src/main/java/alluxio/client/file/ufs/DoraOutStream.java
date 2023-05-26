package alluxio.client.file.ufs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.DoraCacheClient;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;

/**
 * A Dora output stream.
 */
public class DoraOutStream extends FileOutStream {

  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final FileSystemContext mContext;
  private final OutStreamOptions mOptions;
  private boolean mClosed;
  protected final AlluxioURI mUri;
  private final DoraCacheClient mDoraClient;

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   * @param doraClient the client saved to do close()
   */
  public DoraOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context,
                       DoraCacheClient doraClient)
      throws IOException {
    mCloser = Closer.create();
    // Acquire a resource to block FileSystemContext reinitialization, this needs to be done before
    // using mContext.
    // The resource will be released in close().
    mContext = context;
    mCloser.register(mContext.blockReinit());
    mDoraClient = doraClient;
    try {
      mUri = Preconditions.checkNotNull(path, "path");
      mOptions = options;
      mClosed = false;
      mBytesWritten = 0;
    } catch (Throwable t) {
      throw CommonUtils.closeAndRethrow(mCloser, t);
    }
  }

  @Override
  public void write(int b) throws IOException {
     // Add write implementation here.
  }

  @Override
  public void close() {
    if (!mClosed) {
      CompleteFilePOptions options = CompleteFilePOptions.newBuilder()
          .setUfsLength(12345)//getBytesWritten())
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().build())
          .setContentHash("HASH-256") // compute hash here
          .build();
      mClosed = true;
      mDoraClient.completeFile(mUri.getPath(), options);
    }
  }
}
