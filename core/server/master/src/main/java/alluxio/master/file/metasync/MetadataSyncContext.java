package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.RpcContext;

import javax.annotation.Nullable;

public class MetadataSyncContext {
  private final boolean mIsRecursive;

  private final RpcContext mRpcContext;

  private FileSystemMasterCommonPOptions mCommonOptions;

  // TODO: need to format the startAfter like what we did for partial listing
  @Nullable
  private String mStartAfter;

  public MetadataSyncContext(
      boolean isRecursive, RpcContext rpcContext, FileSystemMasterCommonPOptions commonOptions, @Nullable String startAfter) {
    mIsRecursive = isRecursive;
    mRpcContext = rpcContext;
    mCommonOptions = commonOptions;
    if (startAfter == null) {
      mStartAfter = null;
    } else {
      if (startAfter.startsWith(AlluxioURI.SEPARATOR)) {
        throw new RuntimeException("relative path is expected");
      }
      mStartAfter = startAfter;
      // TODO do something like this
      /*
      if (startAfter.startsWith(AlluxioURI.SEPARATOR)) {
        // this path starts from the root, so we must remove the prefix
        String startAfterCheck = startAfter.substring(0,
            Math.min(path.getPath().length(), startAfter.length()));
        if (!path.getPath().startsWith(startAfterCheck)) {
          throw new InvalidPathException(
              ExceptionMessage.START_AFTER_DOES_NOT_MATCH_PATH
                  .getMessage(startAfter, path.getPath()));
        }
        startAfter = startAfter.substring(
            Math.min(startAfter.length(), path.getPath().length()));
      }
       */
    }
  }

  public boolean isRecursive() {
    return mIsRecursive;
  }

  public RpcContext getRpcContext() {
    return mRpcContext;
  }

  public FileSystemMasterCommonPOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return null if the listing starts from the beginning, otherwise the start after path inclusive
   */
  @Nullable
  public String getStartAfter() {
    return mStartAfter;
  }
}
