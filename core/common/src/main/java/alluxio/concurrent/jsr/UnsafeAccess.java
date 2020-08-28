/*
 * Written by Stefan Zobel and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package alluxio.concurrent.jsr;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

class UnsafeAccess {

  static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private UnsafeAccess() {}
}
