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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A wrapper class to translate Hadoop FileSystem to Alluxio FileSystem.
 * This class is only to be used internally and most methods are not implemented.
 */
public class AlluxioHdfsFileSystem implements alluxio.client.file.FileSystem {
  private final org.apache.hadoop.fs.FileSystem mFileSystem;

  /**
   * @param fileSystem file system
   */
  public AlluxioHdfsFileSystem(org.apache.hadoop.fs.FileSystem fileSystem) {
    mFileSystem = Preconditions.checkNotNull(fileSystem, "fileSystem");
  }

  @Override
  public void createDirectory(AlluxioURI alluxioURI, CreateDirectoryPOptions options)
      throws IOException {
    FsPermission permission = new FsPermission(Mode.fromProto(options.getMode()).toShort());
    mFileSystem.mkdirs(HadoopUtils.toPath(alluxioURI), permission);
  }

  @Override
  public FileInStream openFile(AlluxioURI alluxioURI, OpenFilePOptions options) throws IOException {
    return new AlluxioHdfsInputStream(mFileSystem.open(HadoopUtils.toPath(alluxioURI)));
  }

  @Override
  public FileInStream openFile(URIStatus uriStatus, OpenFilePOptions options) throws IOException {
    return new AlluxioHdfsInputStream(mFileSystem.open(new Path(uriStatus.getPath())));
  }

  @Override
  public FileOutStream createFile(AlluxioURI alluxioURI, CreateFilePOptions options) {
    throw new UnsupportedOperationException("CreateFile operation is not supported");
  }

  @Override
  public void delete(AlluxioURI alluxioURI, DeletePOptions options) {
    throw new UnsupportedOperationException("Delete operation is not supported");
  }

  @Override
  public boolean exists(AlluxioURI alluxioURI, ExistsPOptions options) {
    throw new UnsupportedOperationException("Exists operation is not supported");
  }

  @Override
  public AlluxioConfiguration getConf() {
    return InstancedConfiguration.defaults();
  }

  @Override
  public URIStatus getStatus(AlluxioURI alluxioURI, GetStatusPOptions options) throws IOException {
    return HadoopUtils.toUriStatus(mFileSystem.getFileStatus(HadoopUtils.toPath(alluxioURI)));
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI alluxioURI, ListStatusPOptions options)
      throws IOException {
    return Arrays.stream(mFileSystem.listStatus(HadoopUtils.toPath(alluxioURI))).map(HadoopUtils::toUriStatus)
        .collect(Collectors.toList());
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException("isClosed is not supported");
  }

  @Override
  public void free(AlluxioURI alluxioURI, FreePOptions options) {
    throw new UnsupportedOperationException("Free is not supported.");
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI alluxioURI) {
    throw new UnsupportedOperationException("GetBlockLocations is not supported.");
  }

  @Override
  public void mount(AlluxioURI alluxioURI1, AlluxioURI alluxioURI2, MountPOptions options) {
    throw new UnsupportedOperationException("Mount is not supported.");
  }

  @Override
  public void updateMount(AlluxioURI alluxioURI, MountPOptions options) {
    throw new UnsupportedOperationException("UpdateMount is not supported.");
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() {
    throw new UnsupportedOperationException("GetMountTable is not supported.");
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() {
    throw new UnsupportedOperationException("GetSyncPathList is not supported.");
  }

  @Override
  public void persist(AlluxioURI alluxioURI, ScheduleAsyncPersistencePOptions options) {
    throw new UnsupportedOperationException("Persist is not supported.");
  }

  @Override
  public void rename(AlluxioURI source, AlluxioURI destination, RenamePOptions options) {
    throw new UnsupportedOperationException("ReverseResolve is not supported.");
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI alluxioURI) {
    throw new UnsupportedOperationException("ReverseResolve is not supported.");
  }

  @Override
  public void setAcl(AlluxioURI alluxioURI, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) {
    throw new UnsupportedOperationException("SetAcl is not supported.");
  }

  @Override
  public void startSync(AlluxioURI alluxioURI) {
    throw new UnsupportedOperationException("StartSync is not supported.");
  }

  @Override
  public void stopSync(AlluxioURI alluxioURI) {
    throw new UnsupportedOperationException("StopSync is not supported.");
  }

  @Override
  public void setAttribute(AlluxioURI alluxioURI, SetAttributePOptions options) {
    throw new UnsupportedOperationException("SetAttribute is not supported.");
  }

  @Override
  public void unmount(AlluxioURI alluxioURI, UnmountPOptions options) {
    throw new UnsupportedOperationException("unmount is not supported.");
  }

  @Override
  public void close() throws IOException {
    mFileSystem.close();
  }
}
