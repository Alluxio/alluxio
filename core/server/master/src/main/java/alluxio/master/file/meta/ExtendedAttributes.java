package alluxio.master.file.meta;

/**
 * A class used to build identifier keys for an extended attribute.
 *
 * Extended attributes take the form of
 * {@code {NAMESPACE}{SEPARATOR}{ATTRIBUTE_TYPE}{SEPARATOR}{IDENTIFIER}}
 *
 * For example, the persis
 */
public class ExtendedAttributes {
  private static final String SEPARATOR = ".";
  private static final String SYSTEM_PREFIX = "s";

  private static final String PERSISTENCE_STATE_COMPONENT = "ps";
  private static final String PERSISTENCE_STATE_PREFIX =
      buildAttr(SYSTEM_PREFIX, PERSISTENCE_STATE_COMPONENT);

  /**
   * Get the identifier for the persistence state identified by the provided component.
   *
   * @return the name of the persistence state attribute
   */
  public static String getPersistenceStateName() {
    return PERSISTENCE_STATE_PREFIX;
  }

  /**
   * Builds an attribute by joining namespace components together.
   *
   * @param components the components to join
   * @return the attribute joined by {@link #SEPARATOR}
   */
  private static String buildAttr(String... components) {
    return String.join(SEPARATOR, components);
  }
}
