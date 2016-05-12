package alluxio.master.file.options;

import alluxio.thrift.LoadMetadataTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class LoadMetadataOptions {
  private boolean mRecursive;
  private boolean mLoadDirectChildren;

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new LoadMetadataOptions();
  }

  private LoadMetadataOptions() {
    mRecursive = false;
    mLoadDirectChildren = false;
  }

  public LoadMetadataOptions(LoadMetadataTOptions options) {
    this();
    mRecursive = options.isRecursive();
    mLoadDirectChildren = options.isLoadDirectChildren();
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the load direct children flag. It specifies whether the direct children should
   * be loaded.
   */
  public boolean isLoadDirectChildren() {
    return mLoadDirectChildren;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public LoadMetadataOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * Sets the load direct children flag.
   *
   * @param loadDirectChildren the load direct children flag. It specifies whether the direct
   *                           children should be loaded.
   * @return the updated object
   */
  public LoadMetadataOptions setLoadDirectChildren(boolean loadDirectChildren) {
    mLoadDirectChildren = loadDirectChildren;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadMetadataOptions)) {
      return false;
    }
    LoadMetadataOptions that = (LoadMetadataOptions) o;
    return Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mLoadDirectChildren, that.mLoadDirectChildren);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive, mLoadDirectChildren);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("recursive", mRecursive)
        .add("loadDirectChildren", mLoadDirectChildren).toString();
  }
}