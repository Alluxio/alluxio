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

package alluxio.jnifuse.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to load the shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);
  //singleton
  private static final NativeLibraryLoader INSTANCE = new NativeLibraryLoader();
  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private static final String SHARED_LIBRARY_NAME =
      Environment.getSharedLibraryName("jnifuse");
  private static final String SHARED_LIBRARY_FILE_NAME =
      Environment.getSharedLibraryFileName("jnifuse");
  private static final String JNI_LIBRARY_NAME =
      Environment.getJniLibraryName("jnifuse");
  private static final String JNI_LIBRARY_FILE_NAME =
      Environment.getJniLibraryFileName("jnifuse");
  private static final String TEMP_FILE_PREFIX = "libjnifuse";
  private static final String TEMP_FILE_SUFFIX =
      Environment.getJniLibraryExtension();

  private static void setInitialized(boolean initialized) {
    INITIALIZED.set(initialized);
  }

  /**
   * Get a reference to the NativeLibraryLoader.
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return INSTANCE;
  }

  /**
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath.
   * {@link NativeLibraryLoader#loadLibraryFromJar(String)}
   *
   * @param tmpDir A temporary directory to use
   *               to copy the native library to when loading from the classpath.
   *               If null, or the empty string, we rely on Java's
   *               {@link File#createTempFile(String, String)}
   *               function to provide a temporary location.
   *               The temporary file will be registered for deletion
   *               on exit.
   * @throws IOException if a filesystem operation fails
   */
  public synchronized void loadLibrary(final String tmpDir) throws IOException {
    try {
      System.loadLibrary(SHARED_LIBRARY_NAME);
      LOG.info("Loaded {} by System.loadLibrary.", SHARED_LIBRARY_NAME);
    } catch (final UnsatisfiedLinkError ule1) {
      try {
        System.loadLibrary(JNI_LIBRARY_NAME);
        LOG.info("Loaded {} by System.loadLibrary.", JNI_LIBRARY_NAME);
      } catch (final UnsatisfiedLinkError ule2) {
        loadLibraryFromJar(tmpDir);
      }
    }
  }

  /**
   * Attempts to extract the native library
   * from the classpath and load it.
   *
   * @param tmpDir A temporary directory to use
   *               to copy the native library to. If null,
   *               or the empty string, we rely on Java's
   *               {@link File#createTempFile(String, String)}
   *               function to provide a temporary location.
   *               The temporary file will be registered for deletion
   *               on exit.
   * @throws IOException if a filesystem operation fails
   */
  void loadLibraryFromJar(final String tmpDir)
      throws IOException {
    if (!INITIALIZED.get()) {
      String libPath = loadLibraryFromJarToTemp(tmpDir).getAbsolutePath();
      System.load(libPath);
      setInitialized(true);
      LOG.info("Loaded lib by jar from path {}.", libPath);
    }
  }

  File loadLibraryFromJarToTemp(final String tmpDir)
      throws IOException {
    final File temp;
    if (tmpDir == null || tmpDir.isEmpty()) {
      temp = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
    } else {
      temp = new File(tmpDir, JNI_LIBRARY_FILE_NAME);
      if (temp.exists() && !temp.delete()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " already exists and cannot be removed.");
      }
      if (!temp.createNewFile()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " could not be created.");
      }
    }

    if (!temp.exists()) {
      throw new RuntimeException(
          "File " + temp.getAbsolutePath() + " does not exist.");
    } else {
      temp.deleteOnExit();
    }

    // attempt to copy the library from the Jar file to the temp destination
    try (final InputStream is = getClass().getClassLoader()
        .getResourceAsStream(SHARED_LIBRARY_FILE_NAME)) {
      if (is == null) {
        throw new RuntimeException(
            SHARED_LIBRARY_FILE_NAME + " was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }

    return temp;
  }

  /**
   * Private constructor to disallow instantiation.
   */
  private NativeLibraryLoader() {
  }
}
