package alluxio.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class PlaintextCredentialConfiguration implements CredentialConfiguration{
  private static final Logger LOG = LoggerFactory.getLogger(PlaintextCredentialConfiguration.class);

  /**
   * Map of user-specified properties. When key is mapped to Optional.empty(), it indicates no
   * value is set for this key. Note that, ConcurrentHashMap requires not null for key and value.
   */
  // TODO(jiacheng): Optional? Use AlluxioProperties?
  private final ConcurrentHashMap<PropertyKey, String> mCredProps =
          new ConcurrentHashMap<>();

  @Override
  public String get(PropertyKey key) {
    if (!mCredProps.containsKey(key)) {
      throw new RuntimeException();
    }
    return mCredProps.get(key);
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return mCredProps.containsKey(key);
  }

  @Override
  public void set(PropertyKey key, Object value) {
    mCredProps.put(key, value.toString());
  }
}
