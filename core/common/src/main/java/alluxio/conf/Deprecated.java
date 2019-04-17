package alluxio.conf;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation for PropertyKeys that can be used to mark a key as deprecated (slated for
 * removal). If a key is marked with this annotation, then any Alluxio processes should trigger
 * log warning messages upon configuration validation.
 *
 * This annotation is provided as an alternative to {@link java.lang.Deprecated}. Both
 * annotations do not need to be specified on a key. The advantage of using this annotation is
 * that we can supply a custom message rather than simply telling the user "This key has been
 * deprecated".
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Deprecated {
  /**
   * A message to the user which could suggest an alternative key.
   *
   * The message should not mention anything about deprecation, but rather alternatives,
   * side-effects, and/or the time-frame for removal of the key.
   *
   * An example may be:
   *
   * "This key is slated for removal in v2.0. An equivalent configuration property is
   * alluxio.x.y.z."
   *
   * or
   *
   * "This key is slated for removal in v3.0. Afterwards, this value will no longer be
   * configurable"
   *
   * @return A string explaining deprecation or removal
   */
  String message() default "";
}
