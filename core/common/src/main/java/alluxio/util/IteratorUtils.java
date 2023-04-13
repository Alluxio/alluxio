package alluxio.util;

import java.io.IOException;
import java.util.Iterator;

public class IteratorUtils {
  public static <T> T nextOrNull(Iterator<T> iterator) {
    if (iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }

  public static <T> T nextOrNullUnwrapIOException(Iterator<T> iterator) throws IOException {
    try {
      if (iterator.hasNext()) {
        return iterator.next();
      }
      return null;
    } catch (Exception e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
  }
}

