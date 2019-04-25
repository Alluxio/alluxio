package alluxio.conf;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Version is a hex encoded MD5 hash of the cluster or path configurations.
 */
@NotThreadSafe
public final class Version {
  private MessageDigest mMD5;
  private Supplier<Stream<byte[]>> mProperties;
  private String mVersion;
  private boolean mShouldUpdate;

  /**
   * @param properties a stream of encoded properties
   */
  public Version(Supplier<Stream<byte[]>> properties) {
    try {
      mMD5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    mProperties = properties;
    mVersion = "";
    mShouldUpdate = true;
  }

  /**
   * Called when the version needs to be updated.
   * Internally, it just sets a flag to indicate that the version should be updated,
   * but version will not be recomputed until {@link #get()} is called.
   */
  public void update() {
    mShouldUpdate = true;
  }

  private String compute() {
    mMD5.reset();
    mProperties.get().forEach(property -> mMD5.update(property));
    return Hex.encodeHexString(mMD5.digest());
  }

  /**
   * If {@link #update()} is called since last {@link #get()}, then the version will be recomputed,
   * otherwise, the internally cached version is returned.
   *
   * @return the latest version
   */
  public String get() {
    if (mShouldUpdate) {
      mVersion = compute();
      mShouldUpdate = false;
    }
    return mVersion;
  }
}
