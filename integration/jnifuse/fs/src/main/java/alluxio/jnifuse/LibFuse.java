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

package alluxio.jnifuse;

import alluxio.jnifuse.utils.LibfuseVersion;
import alluxio.jnifuse.utils.NativeLibraryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Load the libfuse library.
 */
public class LibFuse {
  private static final Logger LOG = LoggerFactory.getLogger(LibFuse.class);

  /**
   * The libfuse load strategy.
   */
  public enum LibfuseLoadStrategy {
    LOAD_FUSE2_ONLY,
    LOAD_FUSE3_ONLY,
    FUSE3_THEN_FUSE2,
  }

  public native int fuse_main_real(AbstractFuseFileSystem fs, int argc, String[] argv);

  public native ByteBuffer fuse_get_context();

  /**
   * Load the Libfuse library with the target load strategy.
   *
   * @param strategy the load strategy
   * @return the loaded libfuse version
   */
  public static LibfuseVersion loadLibrary(LibfuseLoadStrategy strategy) {
    String tmpDir = System.getenv("JNIFUSE_SHAREDLIB_DIR");
    LibfuseVersion versionToLoad = strategy == LibfuseLoadStrategy.LOAD_FUSE2_ONLY
        ? LibfuseVersion.VERSION_2 : LibfuseVersion.VERSION_3;
    RuntimeException runtimeException;
    try {
      NativeLibraryLoader.getInstance().loadLibrary(versionToLoad, tmpDir);
      return versionToLoad;
    } catch (RuntimeException re) {
      runtimeException = re;
    }
    if (strategy == LibfuseLoadStrategy.FUSE3_THEN_FUSE2) {
      LOG.error("Failed to load jnifuse with libfuse 3, try loading libfuse 2", runtimeException);
      try {
        NativeLibraryLoader.getInstance().loadLibrary(LibfuseVersion.VERSION_2, tmpDir);
        return LibfuseVersion.VERSION_2;
      } catch (RuntimeException re) {
        runtimeException = re;
      }
    }
    throw runtimeException;
  }
}
