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

package alluxio.client.modules;

import alluxio.client.file.dora.DefaultDoraCacheClientFactory;
import alluxio.client.file.dora.DoraCacheClientFactory;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.conf.Configuration;

import com.google.inject.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice module for dora client filesystem.
 */
public class DoraFileSystemModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DoraFileSystemModule.class);

  @Override
  protected void configure() {
    bind(DoraCacheClientFactory.class).to(DefaultDoraCacheClientFactory.class);
    bind(FileSystemOptions.class)
        .toInstance(FileSystemOptions.Builder.fromConf(Configuration.global()).build());
  }
}
