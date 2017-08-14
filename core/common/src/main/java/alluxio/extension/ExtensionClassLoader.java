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

package alluxio.extension;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Interface for a client in the Alluxio system.
 */
public class ExtensionClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionClassLoader.class);

  public ExtensionClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    URL resource = findResource(name.replace('.', '/') + ".class");
    if (resource != null) {
      try (InputStream is = resource.openStream()) {
        byte[] bytecode = IOUtils.toByteArray(is);
        String packageName = getPackageName(name);
        if (packageName != null && getPackage(packageName) == null) {
          // Define package if not already defined
          // TODO (adit): version package from manifest?
          definePackage(packageName, null, null, null, null, null, null, null);
        }
        return defineClass(name, bytecode, 0, bytecode.length);
      } catch (IOException e) {
        LOG.warn("Failed to read class definition for class {}", name, e);
      }
    }
    // Delegate to parent if class not found in extension URL
    return super.findClass(name);
  }

  protected String getPackageName(String className) {
    int idx = className.lastIndexOf('.');
    return idx >= 0 ? className.substring(0, idx) : null;
  }
}
