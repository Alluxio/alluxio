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

package alluxio.fuse.auth;

import static org.mockito.ArgumentMatchers.eq;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.ListStatusPartialResult;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.jnifuse.FuseFileSystem;
import alluxio.jnifuse.struct.FuseContext;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public abstract class AbstractAuthPolicyTest {
  protected UserGroupFileSystem mFileSystem = new UserGroupFileSystem();
  protected AuthPolicy mAuthPolicy;
  protected CustomContextFuseFileSystem mFuseFileSystem = new CustomContextFuseFileSystem();

  @Test
  public void setUserGroup() throws Exception {
    long uid = 123;
    long gid = 456;
    String userName = "myuser";
    String groupName = "mygroup";
    PowerMockito.spy(AlluxioFuseUtils.class);
    PowerMockito.when(AlluxioFuseUtils.getUserName(eq(uid)))
        .thenReturn(Optional.of(userName));
    PowerMockito.when(AlluxioFuseUtils.getGroupName(eq(gid)))
        .thenReturn(Optional.of(groupName));
    AlluxioURI uri = new AlluxioURI("/TestSetUserGroup");
    mAuthPolicy.setUserGroup(uri, uid, gid);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(userName, status.getOwner());
    Assert.assertEquals(groupName, status.getGroup());

    // `setAttribute` should not be called once more as the file
    // already has correct owner and group, as `UserGroupFileSystem.setAttribute`
    // defined here creates a new `URIStatus` each time,
    // `mFileSystem.getStatus` should return the same instance if
    // `setAttribute` is not called once again.
    mAuthPolicy.setUserGroup(uri, uid, gid);
    URIStatus status2 = mFileSystem.getStatus(uri);
    Assert.assertSame(status, status2);
  }

  static class CustomContextFuseFileSystem implements FuseFileSystem {
    private FuseContext mContext;

    public void setContext(FuseContext context) {
      mContext = context;
    }

    @Override
    public FuseContext getContext() {
      return mContext;
    }
  }

  /**
   * An implementation of the file system that supports set user group of file
   * and get the user group file information.
   */
  static class UserGroupFileSystem implements FileSystem {
    private final Map<AlluxioURI, URIStatus> mFiles = new HashMap<>();

    public UserGroupFileSystem() {}

    @Override
    public boolean isClosed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(AlluxioURI path, CheckAccessPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(AlluxioURI path, DeletePOptions options)
        throws IOException,
        AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(AlluxioURI path, ExistsPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void free(AlluxioURI path, FreePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(URIStatus status)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AlluxioConfiguration getConf() {
      return Configuration.global();
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
        throws IOException, AlluxioException {
      if (mFiles.containsKey(path)) {
        return mFiles.get(path);
      } else {
        throw new FileDoesNotExistException(path);
      }
    }

    @Override
    public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
        Consumer<? super URIStatus> action) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
        throws  IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListStatusPartialResult listStatusPartial(
        AlluxioURI path, ListStatusPartialPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void loadMetadata(AlluxioURI path, ListStatusPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateMount(AlluxioURI alluxioPath, MountPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, MountPointInfo> getMountTable(boolean checkUfs)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SyncPointInfo> getSyncPathList() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileInStream openFile(URIStatus status, OpenFilePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AlluxioURI reverseResolve(AlluxioURI ufsUri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
        SetAclPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void startSync(AlluxioURI path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopSync(AlluxioURI path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(AlluxioURI path, SetAttributePOptions options)
        throws IOException, AlluxioException {
      FileInfo info = new FileInfo();
      info.setFileId(path.toString().hashCode());
      if (options.hasOwner()) {
        info.setOwner(options.getOwner());
      }
      if (options.hasGroup()) {
        info.setGroup(options.getGroup());
      }
      mFiles.put(path, new URIStatus(info));
    }

    @Override
    public void unmount(AlluxioURI path, UnmountPOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
