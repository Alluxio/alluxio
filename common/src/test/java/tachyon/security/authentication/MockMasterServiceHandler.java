/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.authentication;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;

import tachyon.security.RemoteClientUser;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.Command;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;

/**
 * The mocked Master server program for test.
 *
 * In order to test whether the client user is saved in the threadlocal variable of server
 * side, we implement one method user_getUfsAddress() for getting the user maintained in server
 * and return it to client for verification. It is used in the tests of {@link tachyon.security
 * .authentication.AuthenticationFactoryTest}.
 *
 * It needs to be noted that the method name does not indicate its behavior.
 * Other overridden methods are no-op methods.
 */
public class MockMasterServiceHandler implements MasterService.Iface{
  @Override
  public boolean addCheckpoint(long workerId, int fileId, long length,
      String checkpointPath) throws FileDoesNotExistException, SuspectedFileSizeException,
      BlockInfoException, TException {
    return false;
  }

  @Override
  public List<ClientWorkerInfo> getWorkersInfo() throws TException {
    return null;
  }

  @Override
  public List<ClientFileInfo> liststatus(String path) throws InvalidPathException,
      FileDoesNotExistException, TException {
    return null;
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlocks) throws
      BlockInfoException, TException {
    return 0;
  }

  @Override
  public Command worker_heartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlockIds) throws BlockInfoException,
      TException {
    return null;
  }

  @Override
  public void worker_cacheBlock(long workerId, long usedBytesOnTier, long storageDirId,
      long blockId, long length) throws FileDoesNotExistException, BlockInfoException, TException {

  }

  @Override
  public Set<Integer> worker_getPinIdList() throws TException {
    return null;
  }

  @Override
  public List<Integer> worker_getPriorityDependencyList() throws TException {
    return null;
  }

  @Override
  public int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws
      InvalidPathException, FileDoesNotExistException, FileAlreadyExistException,
      BlockInfoException, TachyonException, TException {
    return 0;
  }

  @Override
  public ClientDependencyInfo user_getClientDependencyInfo(int dependencyId) throws
      DependencyDoesNotExistException, TException {
    return null;
  }

  @Override
  public void user_reportLostFile(int fileId) throws FileDoesNotExistException, TException {

  }

  @Override
  public void user_requestFilesInDependency(int depId) throws DependencyDoesNotExistException,
      TException {

  }

  @Override
  public int user_createFile(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws FileAlreadyExistException, InvalidPathException,
      BlockInfoException, SuspectedFileSizeException, TachyonException, TException {
    return 0;
  }

  @Override
  public long user_createNewBlock(int fileId) throws FileDoesNotExistException, TException {
    return 0;
  }

  @Override
  public void user_completeFile(int fileId) throws FileDoesNotExistException, TException {

  }

  @Override
  public long user_getUserId() throws TException {
    return 0;
  }

  @Override
  public long user_getBlockId(int fileId, int index) throws FileDoesNotExistException, TException {
    return 0;
  }

  @Override
  public long user_getCapacityBytes() throws TException {
    return 0;
  }

  @Override
  public long user_getUsedBytes() throws TException {
    return 0;
  }

  @Override
  public NetAddress user_getWorker(boolean random, String host) throws NoWorkerException,
      TException {
    return null;
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, String path) throws InvalidPathException,
      TException {
    return null;
  }

  @Override
  public ClientBlockInfo user_getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      BlockInfoException, TException {
    return null;
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocks(int fileId,
      String path) throws FileDoesNotExistException, InvalidPathException, TException {

    return null;
  }

  @Override
  public boolean user_delete(int fileId, String path, boolean recursive) throws TachyonException,
      TException {
    return false;
  }

  @Override
  public boolean user_rename(int fileId, String srcPath, String dstPath) throws
      FileAlreadyExistException, FileDoesNotExistException, InvalidPathException, TException {
    return false;
  }

  @Override
  public void user_setPinned(int fileId, boolean pinned) throws FileDoesNotExistException,
      TException {

  }

  @Override
  public boolean user_mkdirs(String path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException, TachyonException, TException {
    return false;
  }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata) throws
      FileAlreadyExistException, InvalidPathException, TableColumnException, TachyonException,
      TException {
    return 0;
  }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return 0;
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfo(int id,
      String path) throws TableDoesNotExistException, InvalidPathException, TException {
    return null;
  }

  @Override
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata) throws
      TableDoesNotExistException, TachyonException, TException {

  }

  /**
   * This is the method for test purpose.
   * @return the connected client user's name
   * @throws TException
   */
  @Override
  public String user_getUfsAddress() throws TException {
    return RemoteClientUser.get().getName();
  }

  @Override
  public void user_heartbeat() throws TException {

  }

  @Override
  public boolean user_freepath(int fileId, String path, boolean recursive) throws
      FileDoesNotExistException, TException {
    return false;
  }
}
