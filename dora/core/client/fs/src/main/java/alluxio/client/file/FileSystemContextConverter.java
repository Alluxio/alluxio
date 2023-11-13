package alluxio.client.file;

/**
 * the interface of FileSystemContextConverter.
 */
public class FileSystemContextConverter {
  /**
   * Convert context to another {@link FileSystemContext}. Do nothing by default.
   * @param context
   * @return context
   */
  public FileSystemContext convert(FileSystemContext context) {
    return context;
  }
}
