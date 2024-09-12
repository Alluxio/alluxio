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

package alluxio.util;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodType.methodType;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

/**
 * sun.misc.Cleaner has moved in OpenJDK 9 and
 * sun.misc.Unsafe#invokeCleaner(ByteBuffer) is the replacement.
 * This class is a hack to use sun.misc.Cleaner in Java 8 and
 * use the replacement in Java 9+.
 * This implementation is borrowed from HADOOP-12760.
 */
public final class CleanerUtils {

  // Prevent instantiation
  private CleanerUtils() {}

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;

  /**
   * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason
   * why unmapping is not supported.
   */
  public static final String UNMAP_NOT_SUPPORTED_REASON;

  private static final BufferCleaner CLEANER;

  /**
   * Reference to a BufferCleaner that does unmapping.
   * @return null if not supported
   */
  public static BufferCleaner getCleaner() {
    return CLEANER;
  }

  static {
    final Object hack = AccessController.doPrivileged(
        (PrivilegedAction<Object>) CleanerUtils::unmapHackImpl);
    if (hack instanceof BufferCleaner) {
      CLEANER = (BufferCleaner) hack;
      UNMAP_SUPPORTED = true;
      UNMAP_NOT_SUPPORTED_REASON = null;
    } else {
      CLEANER = null;
      UNMAP_SUPPORTED = false;
      UNMAP_NOT_SUPPORTED_REASON = hack.toString();
    }
  }

  private static Object unmapHackImpl() {
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      try {
        // *** sun.misc.Unsafe unmapping (Java 9+) ***
        final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        // first check if Unsafe has the right method, otherwise we can
        // give up without doing any security critical stuff:
        final MethodHandle unmapper = lookup.findVirtual(unsafeClass,
            "invokeCleaner", methodType(void.class, ByteBuffer.class));
        // fetch the unsafe instance and bind it to the virtual MH:
        final Field f = unsafeClass.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        final Object theUnsafe = f.get(null);
        return newBufferCleaner(ByteBuffer.class, unmapper.bindTo(theUnsafe));
      } catch (SecurityException se) {
        // rethrow to report errors correctly (we need to catch it here,
        // as we also catch RuntimeException below!):
        throw se;
      } catch (ReflectiveOperationException | RuntimeException e) {
        // *** sun.misc.Cleaner unmapping (Java 8) ***
        final Class<?> directBufferClass =
            Class.forName("java.nio.DirectByteBuffer");

        final Method m = directBufferClass.getMethod("cleaner");
        m.setAccessible(true);
        final MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
        final Class<?> cleanerClass =
            directBufferCleanerMethod.type().returnType();

        /*
         * "Compile" a MethodHandle that basically is equivalent
         * to the following code:
         *
         * void unmapper(ByteBuffer byteBuffer) {
         *   sun.misc.Cleaner cleaner =
         *       ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
         *   if (Objects.nonNull(cleaner)) {
         *     cleaner.clean();
         *   } else {
         *     // the noop is needed because MethodHandles#guardWithTest
         *     // always needs ELSE
         *     noop(cleaner);
         *   }
         * }
         */
        final MethodHandle cleanMethod = lookup.findVirtual(
            cleanerClass, "clean", methodType(void.class));
        final MethodHandle nonNullTest = lookup.findStatic(Objects.class,
            "nonNull", methodType(boolean.class, Object.class))
                .asType(methodType(boolean.class, cleanerClass));
        final MethodHandle noop = dropArguments(
            constant(Void.class, null).asType(methodType(void.class)),
            0, cleanerClass);
        final MethodHandle unmapper = filterReturnValue(
            directBufferCleanerMethod,
            guardWithTest(nonNullTest, cleanMethod, noop))
            .asType(methodType(void.class, ByteBuffer.class));
        return newBufferCleaner(directBufferClass, unmapper);
      }
    } catch (SecurityException se) {
      return "Unmapping is not supported, because not all required "
          + "permissions are given to the Alluxio JAR file: " + se
          + " [Please grant at least the following permissions: "
          + "RuntimePermission(\"accessClassInPackage.sun.misc\") "
          + " and ReflectPermission(\"suppressAccessChecks\")]";
    } catch (ReflectiveOperationException | RuntimeException e) {
      return "Unmapping is not supported on this platform, "
          + "because internal Java APIs are not compatible with "
          + "this Alluxio version: " + e;
    }
  }

  private static BufferCleaner newBufferCleaner(
      final Class<?> unmappableBufferClass, final MethodHandle unmapper) {
    assert Objects.equals(
        methodType(void.class, ByteBuffer.class), unmapper.type());
    return buffer -> {
      if (!buffer.isDirect()) {
        throw new IllegalArgumentException(
          "unmapping only works with direct buffers");
      }
      if (!unmappableBufferClass.isInstance(buffer)) {
        throw new IllegalArgumentException("buffer is not an instance of "
            + unmappableBufferClass.getName());
      }
      final Throwable error = AccessController.doPrivileged(
          (PrivilegedAction<Throwable>) () -> {
              try {
                unmapper.invokeExact(buffer);
                return null;
              } catch (Throwable t) {
                return t;
              }
          });
      if (error != null) {
        throw new IOException("Unable to unmap the mapped buffer", error);
      }
    };
  }

  /**
   * Pass in an implementation of this interface to cleanup ByteBuffers.
   * CleanerUtils implements this to allow unmapping of bytebuffers
   * with private Java APIs.
   */
  @FunctionalInterface
  public interface BufferCleaner {
    /**
     * Free buffer.
     * @param b
     * @throws IOException
     */
    void freeBuffer(ByteBuffer b) throws IOException;
  }
}
