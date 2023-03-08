package alluxio.util;

import java.util.Iterator;

public class IteratorUtils {
  public static <T> T nextOrNull(Iterator<T> iterator) {
    if (iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }
}