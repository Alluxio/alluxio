package tachyon.io;

import java.lang.reflect.Field;

import sun.misc.Unsafe;
import tachyon.CommonUtils;

/**
 * Utilities for unsafe operations.
 */
public class UnsafeUtils {
  public static Unsafe getUnsafe()
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {
    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
    theUnsafe.setAccessible(true);
    return (Unsafe) theUnsafe.get(null);
  }

  public static int sByteArrayBaseOffset;

  static {
    try {
      sByteArrayBaseOffset = getUnsafe().arrayBaseOffset(byte[].class);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e) {
      CommonUtils.runtimeException(e);
    }
  }
}
