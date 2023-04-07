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

package alluxio.fuse.cli;

import alluxio.dora.AlluxioURI;
import alluxio.dora.client.file.FileSystemMasterClient;
import alluxio.dora.client.file.ListStatusPartialResult;
import alluxio.dora.client.file.URIStatus;
import alluxio.dora.exception.status.AlluxioStatusException;
import alluxio.dora.exception.status.UnavailableException;
import alluxio.dora.grpc.CheckAccessPOptions;
import alluxio.dora.grpc.CheckConsistencyPOptions;
import alluxio.dora.grpc.CompleteFilePOptions;
import alluxio.dora.grpc.CreateDirectoryPOptions;
import alluxio.dora.grpc.CreateFilePOptions;
import alluxio.dora.grpc.DeletePOptions;
import alluxio.dora.grpc.ExistsPOptions;
import alluxio.dora.grpc.FreePOptions;
import alluxio.dora.grpc.GetStatusPOptions;
import alluxio.dora.grpc.JobProgressReportFormat;
import alluxio.dora.grpc.ListStatusPOptions;
import alluxio.dora.grpc.ListStatusPartialPOptions;
import alluxio.dora.grpc.MountPOptions;
import alluxio.dora.grpc.RenamePOptions;
import alluxio.dora.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.dora.grpc.SetAclAction;
import alluxio.dora.grpc.SetAclPOptions;
import alluxio.dora.grpc.SetAttributePOptions;
import alluxio.dora.grpc.UpdateUfsModePOptions;
import alluxio.dora.job.JobDescription;
import alluxio.dora.job.JobRequest;
import alluxio.dora.security.authorization.AclEntry;
import alluxio.dora.wire.MountPointInfo;
import alluxio.dora.wire.SyncPointInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A mock filesystem master client for tests.
 */
class MockFuseFileSystemMasterClient implements FileSystemMasterClient {
  @Override
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyPOptions options)
      throws AlluxioStatusException {
    return null;
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public URIStatus createFile(AlluxioURI path, CreateFilePOptions options)
      throws AlluxioStatusException {
    return null;
  }

  @Override
  public void completeFile(AlluxioURI path, CompleteFilePOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options) throws AlluxioStatusException {
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws AlluxioStatusException {
    return false;
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options) throws AlluxioStatusException {
  }

  @Override
  public String getFilePath(long fileId) throws AlluxioStatusException {
    return null;
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws AlluxioStatusException {
    return null;
  }

  @Override
  public long getNewBlockIdForFile(AlluxioURI path) throws AlluxioStatusException {
    return 0;
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws AlluxioStatusException {
    return null;
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
      Consumer<? super URIStatus> action) throws AlluxioStatusException {
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws AlluxioStatusException {
    return null;
  }

  @Override
  public ListStatusPartialResult listStatusPartial(
      AlluxioURI path, ListStatusPartialPOptions options) {
    return null;
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public Map<String, MountPointInfo> getMountTable(boolean checkUfs)
      throws AlluxioStatusException {
    return null;
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst) throws AlluxioStatusException {
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws AlluxioStatusException {
    return null;
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws AlluxioStatusException {
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public void startSync(AlluxioURI path) throws AlluxioStatusException {
  }

  @Override
  public void stopSync(AlluxioURI path) throws AlluxioStatusException {
  }

  @Override
  public void scheduleAsyncPersist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public void unmount(AlluxioURI alluxioPath) throws AlluxioStatusException {
  }

  @Override
  public void updateUfsMode(AlluxioURI ufsUri, UpdateUfsModePOptions options)
      throws AlluxioStatusException {
  }

  @Override
  public List<String> getStateLockHolders() throws AlluxioStatusException {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void needsSync(AlluxioURI path) throws AlluxioStatusException {
  }

  @Override
  public Optional<String> submitJob(JobRequest job) {
    return Optional.empty();
  }

  @Override
  public boolean stopJob(JobDescription jobDescription) {
    return false;
  }

  @Override
  public String getJobProgress(JobDescription jobDescription,
      JobProgressReportFormat format, boolean verbose) {
    return null;
  }

  @Override
  public void connect() throws IOException {
  }

  @Override
  public void disconnect() {
  }

  @Override
  public SocketAddress getRemoteSockAddress() throws UnavailableException {
    return null;
  }

  @Override
  public String getRemoteHostName() throws UnavailableException {
    return null;
  }

  @Override
  public InetSocketAddress getConfAddress() throws UnavailableException {
    return null;
  }

  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}
