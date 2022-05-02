package alluxio.worker.block;

import alluxio.AlluxioURI;

import com.google.common.base.Objects;

public class BytesReadMetricKey {

  public AlluxioURI getUri() {
    return mUri;
  }

  private final AlluxioURI mUri;

  public String getUser() {
    return mUser;
  }

  private final String mUser;

  BytesReadMetricKey(AlluxioURI uri, String user) {
    mUri = uri;
    mUser = user;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BytesReadMetricKey that = (BytesReadMetricKey) o;
    return mUri.equals(that.mUri) && mUser.equals(that.mUser);
  }

  @Override public int hashCode() {
    return Objects.hashCode(mUri, mUser);
  }
}
