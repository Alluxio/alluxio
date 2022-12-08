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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is used to load the shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {
  private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);
  //singleton
  private static final NativeLibraryLoader INSTANCE = new NativeLibraryLoader();
  private static final String TEMP_FILE_PREFIX = "libjnifuse";
  private static final String TEMP_FILE_SUFFIX = Environment.getJniLibraryExtension();

  /**
   * Get a reference to the NativeLibraryLoader.
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return INSTANCE;
  }

  /**
   * Internal interface to write tryLoad in an elegant way.
   *
   * @see Runnable interface cannot have checked exception
   */
  interface Load {
    void load() ;
  }

  /**
   * Try running the loading function.
   *
   * @param load the function to load library
   * @return the error if UnsatisfiedLinkError is encountered, empty if it completes successfully
   */
  private Optional<UnsatisfiedLinkError> tryLoad(Load load) {
    try {
      load.load();
      return Optional.empty();
    } catch (final UnsatisfiedLinkError ule) {
      return Optional.of(ule);
    }
  }

  /**
   * Load a version of libjnifuse.
   * <p>
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath.
   * {@link NativeLibraryLoader#loadLibraryFromJar(String, String, String)}
   *
   * @return the error why load is failed. Empty if load is successful
   */
  private Optional<UnsatisfiedLinkError> load(
      final String sharedLibraryName, final String jniLibraryName,
      final String sharedLibraryFileName, final String jniLibraryFileName,
      final String tmpDir) {

    Optional<UnsatisfiedLinkError> err = tryLoad(() -> {
      System.loadLibrary(sharedLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", sharedLibraryName);
    });

    // No error means load is successful.
    // Just return an empty.
    if (!err.isPresent()) {
      return err;
    }

    err = tryLoad(() -> {
      System.loadLibrary(jniLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", jniLibraryName);
    });

    if (!err.isPresent()) {
      return err;
    }

    return tryLoad(() -> loadLibraryFromJar(sharedLibraryFileName, jniLibraryFileName, tmpDir));
  }

  private Optional<UnsatisfiedLinkError> load2(final String tmpDir) {
    final String SHARED_LIBRARY_NAME =
        Environment.getSharedLibraryName("jnifuse");
    final String SHARED_LIBRARY_FILE_NAME =
        Environment.getSharedLibraryFileName("jnifuse");
    final String JNI_LIBRARY_NAME =
        Environment.getJniLibraryName("jnifuse");
    final String JNI_LIBRARY_FILE_NAME =
        Environment.getJniLibraryFileName("jnifuse");

    Optional<UnsatisfiedLinkError> err = load(
        SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME,
        tmpDir
    );

    if (!err.isPresent()) {
      LOG.info("Loaded libjnifuse with libfuse version 2.");
    }

    return err;
  }

  private Optional<UnsatisfiedLinkError> load3(final String tmpDir) {
    final String SHARED_LIBRARY_NAME =
        Environment.getSharedLibraryName("jnifuse3");
    final String SHARED_LIBRARY_FILE_NAME =
        Environment.getSharedLibraryFileName("jnifuse3");
    final String JNI_LIBRARY_NAME =
        Environment.getJniLibraryName("jnifuse3");
    final String JNI_LIBRARY_FILE_NAME =
        Environment.getJniLibraryFileName("jnifuse3");

    Optional<UnsatisfiedLinkError> err = load(
        SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME,
        tmpDir
    );

    if (!err.isPresent()) {
      LOG.info("Loaded libjnifuse with libfuse version 3.");
    }

    return err;
  }

  /**
   * Load the library.
   *
   * @param version     the version of libfuse to load
   * @param tmpDir A temporary directory to use
   *               to copy the native library to when loading from the classpath.
   *               If null, or the empty string, we rely on Java's
   *               {@link File#createTempFile(String, String)}
   *               function to provide a temporary location.
   *               The temporary file will be registered for deletion
   *               on exit.
   */
  public synchronized void loadLibrary(
      final LibfuseVersion version, final String tmpDir) {
    Optional<UnsatisfiedLinkError> err;
    switch (version) {
      case VERSION_2:
        err = load2(tmpDir);
        break;
      case VERSION_3:
        if (!loadToCheckLibraryExistence("libfuse3")) {
          throw new RuntimeException("Failed to find libfuse 3. Please install fuse3");
        }
        err = load3(tmpDir);
        break;
      default:
        // should not fall here
        throw new RuntimeException(String.format("Unsupported libfuse version %d", version));
    }
    if (err.isPresent()) {
      throw err.get();
    }
  }

  /**
   * Attempts to extract the native library
   * from the classpath and load it.
   *
   * @param sharedLibraryFileName The filename of the shared library file
   *                              to be loaded
   * @param jniLibraryFileName    The filename of the jni library
   *                              to be loaded
   * @param tmpDir                A temporary directory to use
   *                              to copy the native library to. If null,
   *                              or the empty string, we rely on Java's
   *                              {@link File#createTempFile(String, String)}
   *                              function to provide a temporary location.
   *                              The temporary file will be registered for deletion
   *                              on exit.
   */
  void loadLibraryFromJar(final String sharedLibraryFileName,
    final String jniLibraryFileName, final String tmpDir) {
    String libPath = loadLibraryFromJarToTemp(
        sharedLibraryFileName, jniLibraryFileName, tmpDir).getAbsolutePath();
    System.load(libPath);
    LOG.info("Loaded lib by jar from path {}.", libPath);
  }

  File loadLibraryFromJarToTemp(final String sharedLibraryFileName,
      final String jniLibraryFileName, final String tmpDir) {
    final File temp;
    try {
      if (tmpDir == null || tmpDir.isEmpty()) {
        temp = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
      } else {
        temp = new File(tmpDir, jniLibraryFileName);
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
          .getResourceAsStream(sharedLibraryFileName)) {
        if (is == null) {
          throw new RuntimeException(
              sharedLibraryFileName + " was not found inside JAR.");
        } else {
          Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return temp;
  }

  private boolean loadToCheckLibraryExistence(String libraryName) {
    Optional<UnsatisfiedLinkError> error = tryLoad(() -> {
      System.loadLibrary(libraryName);
      LOG.info("Loaded {} by System.loadLibrary.", libraryName);
    });
    if (error.isPresent()) {
      throw error.get();
    }
    return true;
  }

  /**
   * Private constructor to disallow instantiation.
   */
  private NativeLibraryLoader() {
  }
}
