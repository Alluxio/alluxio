package alluxio.job;

import alluxio.grpc.LoadJobPOptions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The request of loading files.
 */
@ThreadSafe
public class LoadJobRequest implements JobRequest {
  private static final String TYPE = "Load";
  private final String mPath;
  private final LoadJobPOptions mOptions;

  /**
   * @param path the file path
   * @param options load job options
   **/
  public LoadJobRequest(@JsonProperty("path") String path,
      @JsonProperty("loadJobPOptions") LoadJobPOptions options) {
    mPath = Preconditions.checkNotNull(path, "The file path cannot be null");
    mOptions = Preconditions.checkNotNull(options, "The load job options cannot be null");
  }

  /**
   * @return the file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * @return job options
   */
  public LoadJobPOptions getOptions() {
    return mOptions;
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(this)
        .add("Path", mPath)
        .add("Options", mOptions)
        .toString();
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
