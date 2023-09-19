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

package alluxio.worker.modules;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerIdentity;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provider that resolves worker identities from configuration, persisted storage, or
 * automatically generates a new one.
 */
public class WorkerIdentityProvider implements Provider<WorkerIdentity> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerIdentityProvider.class);
  private final AlluxioConfiguration mConf;

  /**
   * @param conf configuration
   */
  @Inject
  public WorkerIdentityProvider(AlluxioConfiguration conf) {
    mConf = conf;
  }

  /**
   * Resolves a worker's identity from the following sources, in the order of preference:
   * <ol>
   *   <li>
   *     Alluxio configuration. This includes JVM system properties, alluxio-site.properties, etc.
   *   </li>
   *   <li>
   *     Persisted storage. A persistent identity file provided by the user.
   *   </li>
   *   <li>
   *     Automatically generated as a UUID.
   *   </li>
   * </ol>
   *
   * @return worker identity
   */
  @Override
  public WorkerIdentity get() {
    // Look at configurations first
    if (mConf.isSetByUser(PropertyKey.WORKER_IDENTITY_UUID)) {
      String uuidStr = mConf.getString(PropertyKey.WORKER_IDENTITY_UUID);
      return WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuidStr);
    }

    // Try loading from the identity file
    String filePathStr = mConf.getString(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH);
    Path idFile = Paths.get(filePathStr);
    try (BufferedReader reader = Files.newBufferedReader(idFile)) {
      List<String> nonCommentLines = reader.lines()
          .filter(line -> !line.startsWith("#"))
          .collect(Collectors.toList());
      if (nonCommentLines.size() > 0) {
        if (nonCommentLines.size() > 1) {
          LOG.warn("Multiple worker identities configured in {}, only the first one will be used",
              idFile);
        }
        String uuidStr = nonCommentLines.get(0);
        return WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuidStr);
      }
    } catch (FileNotFoundException | NoSuchFileException ignored) {
      // if not existent, proceed to auto generate one
    } catch (IOException e) {
      // in case of other IO error, better stop worker from starting up than use a new identity
      throw new RuntimeException(
          String.format("Failed to read worker identity from identity file %s", idFile), e);
    }

    // No identity is supplied by the user
    // Assume this is the first time the worker starts up, and generate a new one
    UUID generatedId = UUID.randomUUID();
    WorkerIdentity identity = WorkerIdentity.ParserV1.INSTANCE.fromUUID(generatedId);
    try (BufferedWriter writer = Files.newBufferedWriter(idFile)) {
      writer.write("# Worker identity automatically generated at ");
      writer.write(LocalDateTime.now().format(DateTimeFormatter.RFC_1123_DATE_TIME));
      writer.newLine();
      writer.write(generatedId.toString());
      writer.newLine();
    } catch (Exception e) {
      LOG.warn("Failed to persist automatically generated worker identity ({}) to {}, "
          + "this worker will lose its identity after restart", identity, idFile, e);
    }
    try {
      // set the file to be read-only
      Set<PosixFilePermission> permSet = Files.getPosixFilePermissions(idFile);
      Set<PosixFilePermission> nonWritablePermSet = Sets.filter(permSet,
          perm -> perm != PosixFilePermission.OWNER_WRITE
              && perm != PosixFilePermission.GROUP_WRITE
              && perm != PosixFilePermission.OTHERS_WRITE);
      Files.setPosixFilePermissions(idFile, nonWritablePermSet);
    } catch (Exception e) {
      LOG.warn("Failed to set identity file to be read-only", e);
    }
    return identity;
  }
}
