/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse;

import alluxio.jnifuse.AbstractFuseFileSystem;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link AlluxioFuseOpenUtils}.
 */
public class AlluxioFuseOpenUtilsTest {
  final class JNIUtil {
    private  final Map<Object, String> PRIMITIVE_SIGNATURES = new HashMap<>();

    public JNIUtil() {
      PRIMITIVE_SIGNATURES.put(boolean.class, "Z");
      PRIMITIVE_SIGNATURES.put(byte.class, "B");
      PRIMITIVE_SIGNATURES.put(char.class, "C");
      PRIMITIVE_SIGNATURES.put(double.class, "D");
      PRIMITIVE_SIGNATURES.put(float.class, "F");
      PRIMITIVE_SIGNATURES.put(int.class, "I");
      PRIMITIVE_SIGNATURES.put(long.class, "J");
      PRIMITIVE_SIGNATURES.put(short.class, "S");
      PRIMITIVE_SIGNATURES.put(void.class, "V");
    }



    /**
     * Build JNI signature for a method
     *
     * @param m
     * @return
     */
    public  final String getJNIMethodSignature(Method m) {
      final StringBuilder sb = new StringBuilder("(");
      for (final Class<?> p : m.getParameterTypes()) {
        sb.append(getJNIClassSignature(p));
      }
      sb.append(')').append(getJNIClassSignature(m.getReturnType()));
      return sb.toString();
    }

    /**
     * Build JNI signature from a class
     *
     * @param c
     * @return
     */
     String getJNIClassSignature(Class<?> c) {
      if (c.isArray()) {
        final Class<?> ct = c.getComponentType();
        return '[' + getJNIClassSignature(ct);
      } else if (c.isPrimitive()) {
        return PRIMITIVE_SIGNATURES.get(c);
      } else {
        return 'L' + c.getName().replace('.', '/') + ';';
      }
    }
  }

  @Test
  public void cnm() {
    Method m = Arrays.stream(AbstractFuseFileSystem.class.getMethods()).filter(
        it -> it.getName().equals("fsyncCallback")
    ).findFirst().get();
    System.out.println(new JNIUtil().getJNIMethodSignature(m));
  }



  @Test
  public void readOnly() {
    int[] readFlags = new int[]{0x8000, 0x9000};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void writeOnly() {
    int[] readFlags = new int[]{0x8001, 0x9001};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.WRITE_ONLY,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }

  @Test
  public void readWrite() {
    int[] readFlags = new int[]{0x8002, 0x9002, 0xc002};
    for (int readFlag : readFlags) {
      Assert.assertEquals(AlluxioFuseOpenUtils.OpenAction.READ_WRITE,
          AlluxioFuseOpenUtils.getOpenAction(readFlag));
    }
  }
}
