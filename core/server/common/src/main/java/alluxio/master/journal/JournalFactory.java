package alluxio.master.journal;

/**
 * Factory for creating named journals.
 */
public interface JournalFactory {
  /**
   * Creates a new journal using the given name.
   *
   * @param name the journal name
   * @return a new instance of {@link Journal}
   */
  Journal create(String name);
}
