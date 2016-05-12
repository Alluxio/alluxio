package alluxio.master.file.options;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for loading metadata.
 */
@NotThreadSafe
public final class LoadMetadataOptions {
  private boolean mCreateAncestors;
  private boolean mLoadDirectChildren;

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new LoadMetadataOptions();
  }

  private LoadMetadataOptions() {
    mCreateAncestors = false;
    mLoadDirectChildren = false;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isCreateAncestors() {
    return mCreateAncestors;
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
   * @param createAncestors the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public LoadMetadataOptions setCreateAncestors(boolean createAncestors) {
    mCreateAncestors = createAncestors;
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
    return Objects.equal(mCreateAncestors, that.mCreateAncestors)
        && Objects.equal(mLoadDirectChildren, that.mLoadDirectChildren);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateAncestors, mLoadDirectChildren);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("createAncestors", mCreateAncestors)
        .add("loadDirectChildren", mLoadDirectChildren).toString();
  }
}
