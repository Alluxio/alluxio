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

import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.wire.WorkerIdentity;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class WorkerIdentityProviderTest {
  private final UUID mReferenceUuid = UUID.nameUUIDFromBytes(
      "reference uuid".getBytes(StandardCharsets.UTF_8));
  private final UUID mDifferentUuid = UUID.nameUUIDFromBytes(
      "a different uuid".getBytes(StandardCharsets.UTF_8));

  @Rule
  public TemporaryFolder mTempFolder =
      new TemporaryFolder(AlluxioTestDirectory.ALLUXIO_TEST_DIRECTORY);

  private Path mUuidFilePath;

  @Before
  public void setup() throws Exception {
    mUuidFilePath = mTempFolder.newFolder().toPath().resolve("worker_identity_file");
  }

  @Test
  public void preferExplicitConfigurationFirst() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.WORKER_IDENTITY_UUID, mReferenceUuid.toString(), Source.RUNTIME);
    props.set(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath.toString());
    try (BufferedWriter fout = Files.newBufferedWriter(mUuidFilePath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      fout.write(mDifferentUuid.toString());
      fout.newLine();
    }
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf);
    WorkerIdentity identity = provider.get();
    assertEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
    assertNotEquals(mDifferentUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
  }

  @Test
  public void readFromIdFileIfIdNotExplicitlySet() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.WORKER_IDENTITY_UUID, mReferenceUuid.toString(), Source.CLUSTER_DEFAULT);
    props.set(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath.toString());
    try (BufferedWriter fout = Files.newBufferedWriter(mUuidFilePath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      fout.write(mDifferentUuid.toString());
      fout.newLine();
    }
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf);
    WorkerIdentity identity = provider.get();
    assertNotEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
    assertEquals(mDifferentUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
  }

  @Test
  public void readFromWorkingDirIfIdFilePathNotExplicitlySetButFileExists() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath.toString(), Source.DEFAULT);
    try (BufferedWriter fout = Files.newBufferedWriter(mUuidFilePath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      fout.write(mReferenceUuid.toString());
      fout.newLine();
    }
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf);
    WorkerIdentity identity = provider.get();
    assertEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
  }

  @Test
  public void autoGenerateIfIdFilePathNotSetAndFileNotExists() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath.toString(), Source.DEFAULT);
    // put the identity in a file in the same directory but with a different name,
    // which cannot be detected
    Path path = mUuidFilePath.getParent().resolve("not_really_worker_identity");
    try (BufferedWriter fout = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      fout.write(mReferenceUuid.toString());
      fout.newLine();
    }
    assertFalse(Files.exists(mUuidFilePath));
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf, () -> mDifferentUuid);
    WorkerIdentity identity = provider.get();
    assertNotEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
    assertEquals(mDifferentUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
  }

  @Test
  public void autoGeneratedIdPersistedToDesignatedPath() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.set(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath);
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    assertFalse(Files.exists(mUuidFilePath));
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf, () -> mReferenceUuid);
    WorkerIdentity identity = provider.get();
    assertEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));

    // tests if the file gets persisted to the working directory
    assertTrue(Files.exists(mUuidFilePath));
    try (BufferedReader reader = Files.newBufferedReader(mUuidFilePath, StandardCharsets.UTF_8)) {
      List<String> nonCommentLines = reader.lines()
          .filter(line -> !line.startsWith("#"))
          .collect(Collectors.toList());
      assertEquals(1, nonCommentLines.size());
      assertEquals(mReferenceUuid, UUID.fromString(nonCommentLines.get(0)));
    }
  }

  @Test
  public void autoGeneratedIdFileSetToReadOnly() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.set(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath);
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf, () -> mReferenceUuid);
    WorkerIdentity identity = provider.get();
    assertEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
    assertTrue(Files.exists(mUuidFilePath));
    Set<PosixFilePermission> filePerms =
        Files.getPosixFilePermissions(mUuidFilePath);
    Set<PosixFilePermission> writePerms = ImmutableSet.of(OWNER_WRITE, GROUP_WRITE, OTHERS_WRITE);
    assertTrue(Sets.intersection(filePerms, writePerms).isEmpty());
  }

  @Test
  public void shouldNotOverwriteExistingIdFile() throws Exception {
    AlluxioProperties props = new AlluxioProperties();
    props.set(PropertyKey.WORKER_IDENTITY_UUID_FILE_PATH, mUuidFilePath);
    // create the file first, but add only comment lines
    // it should not be overwritten with the auto generated id
    try (BufferedWriter fout = Files.newBufferedWriter(mUuidFilePath)) {
      fout.write("# comment");
      fout.newLine();
    }
    AlluxioConfiguration conf = new InstancedConfiguration(props);
    WorkerIdentityProvider provider = new WorkerIdentityProvider(conf, () -> mReferenceUuid);
    // this should succeed without any exception
    WorkerIdentity identity = provider.get();
    assertEquals(mReferenceUuid, WorkerIdentity.ParserV1.INSTANCE.toUUID(identity));
    // the original id file is left intact
    assertTrue(Files.exists(mUuidFilePath));
    try (BufferedReader reader = Files.newBufferedReader(mUuidFilePath)) {
      String content = IOUtils.toString(reader);
      assertEquals("# comment\n", content);
    }
  }
}
