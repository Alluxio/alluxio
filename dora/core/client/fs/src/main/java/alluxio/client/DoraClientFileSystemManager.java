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

package alluxio.client;

import static java.util.Objects.requireNonNull;

import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.DefaultDoraCacheClientFactory;
import alluxio.client.file.dora.DoraCacheClientFactory;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.client.modules.DoraClientModule;
import alluxio.client.modules.DoraFileSystemModule;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dora client manager.
 */
public class DoraClientFileSystemManager implements ClientFileSystemManager {
  private static DoraClientFileSystemManager sINSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(DoraClientFileSystemManager.class);
  private static final AtomicBoolean CONF_LOGGED = new AtomicBoolean(false);

  private static DoraClientFileSystemManager sINSTANCE;
  private final FileSystemOptions mFileSystemOptions;
  private final DoraCacheClientFactory mDoraClientFactory;

  /**
   * @param conf
   * @return DoraClientManager
   */
  public static DoraClientFileSystemManager get(AlluxioConfiguration conf) {
    if (sINSTANCE != null) {
      return sINSTANCE;
    }
    synchronized (DoraClientFileSystemManager.class) {
      if (sINSTANCE == null) {
        sINSTANCE = create(conf);
      }
    }
    return sINSTANCE;
  }

  private static DoraClientFileSystemManager create(AlluxioConfiguration conf) {
    ImmutableList<Module> modules = ImmutableList.of(new DoraFileSystemModule());
    Injector injector;
    if (conf.isSet(PropertyKey.USER_CLIENT_OVERRIDE_MODULE_CLASS)) {
      Class<? extends AbstractModule> overrideModule =
          conf.getClass(PropertyKey.USER_CLIENT_OVERRIDE_MODULE_CLASS);
      try {
        AbstractModule overrideModuleInstance =
            overrideModule.getDeclaredConstructor().newInstance();
        injector = Guice.createInjector(Modules.override(modules).with(overrideModuleInstance));
      } catch (InstantiationException | IllegalAccessException
               | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    } else {
      injector = Guice.createInjector(modules);
    }
    return injector.getInstance(DoraClientFileSystemManager.class);
  }

  /**
   * Constructor.
   * @param fileSystemOptions
   * @param doraCacheClientFactory
   */
  @Inject
  public DoraClientFileSystemManager(
      FileSystemOptions fileSystemOptions,
      DoraCacheClientFactory doraCacheClientFactory) {
    mFileSystemOptions = requireNonNull(fileSystemOptions, "fileSystemOptions is null");
    mDoraClientFactory = requireNonNull(doraCacheClientFactory,
        "doraCacheClientFactory is null");
  }

  /**
   * @param context
   * @return a new FileSystem instance
   */
  public FileSystem create(FileSystemContext context) {
    return create(context, mFileSystemOptions);
  }

  /**
   * @param context the FileSystemContext to use with the FileSystem
   * @param options FileSystemOptions
   * @return a new FileSystem instance
   */
  public FileSystem create(FileSystemContext context, FileSystemOptions options) {
    AlluxioConfiguration conf = context.getClusterConf();
    checkSortConf(conf);
    Optional<UfsFileSystemOptions> ufsOptions = options.getUfsFileSystemOptions();
    Preconditions.checkArgument(ufsOptions.isPresent(),
        "Missing UfsFileSystemOptions in FileSystemOptions");
    FileSystem fs = new UfsBaseFileSystem(context,
        options.getUfsFileSystemOptions().get());

    if (options.isDoraCacheEnabled()) {
      LOG.debug("Dora cache enabled");
      fs = new DoraCacheFileSystem(fs, context, mDoraClientFactory.create(context));
    }
    return fs;
  }

  private void checkSortConf(AlluxioConfiguration conf) {
    if (LOG.isDebugEnabled() && !CONF_LOGGED.getAndSet(true)) {
      // Sort properties by name to keep output ordered.
      List<PropertyKey> keys = new ArrayList<>(conf.keySet());
      keys.sort(Comparator.comparing(PropertyKey::getName));
      for (PropertyKey key : keys) {
        Object value = conf.getOrDefault(key, null);
        Source source = conf.getSource(key);
        LOG.debug("{}={} ({})", key.getName(), value, source);
      }
    }
  }
}
