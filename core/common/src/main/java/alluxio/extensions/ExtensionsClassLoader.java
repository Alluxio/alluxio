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

package alluxio.extensions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * An isolated {@link ClassLoader} for loading extensions to core Alluxio. This class loader first
 * scans the provided URLs to define a class and in case the class is not found it will fallback to
 * the provided default class loader.
 */
public class ExtensionsClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionsClassLoader.class);

  /**
   * A {@link ClassLoader} to make the protected methods accessible.
   */
  private static class DefaultClassLoader extends ClassLoader {
    /**
     * @param classLoader the parent class loader
     */
    public DefaultClassLoader(ClassLoader classLoader) {
      super(classLoader);
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      return super.findClass(name);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      return super.loadClass(name);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      return super.loadClass(name, resolve);
    }
  }

  private final DefaultClassLoader mDefaultClassloader;

  /**
   * @param urls array of URLs of jars
   * @param defaultClassLoader the default class loader to fall back
   */
  public ExtensionsClassLoader(URL[] urls, ClassLoader defaultClassLoader) {
    // Pass null to override parent first delegation
    super(urls, null);
    mDefaultClassloader = new DefaultClassLoader(defaultClassLoader);
    LOG.debug("Created ExtensionsClassLoader with jars={}", urls);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    try {
      return super.findClass(name);
    } catch (ClassNotFoundException e) {
      return mDefaultClassloader.findClass(name);
    }
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      return super.loadClass(name, resolve);
    } catch (ClassNotFoundException e) {
      return mDefaultClassloader.loadClass(name, resolve);
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = super.getResources(name);
    // Falls back to default class loader if not found in the URL class loader
    if (!resources.hasMoreElements()) {
      LOG.debug("Falling back to default class loader for loading resource {}", name);
      return mDefaultClassloader.getResources(name);
    }
    return resources;
  }
}
