package alluxio.conf;

import alluxio.wire.Property;

import java.util.Properties;

public interface CredentialConfiguration {
  /**
   * Gets the value for the given key in the {@link Properties}; if this key is not found, a
   * RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  String get(PropertyKey key);

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  boolean isSet(PropertyKey key);

  void set(PropertyKey key, Object value);
}
