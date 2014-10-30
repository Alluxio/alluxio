package tachyon.perf;

import org.apache.thrift.TException;

import tachyon.perf.thrift.MasterService;
import tachyon.perf.thrift.SlaveAlreadyRegisterException;
import tachyon.perf.thrift.SlaveNotRegisterException;

/**
 * The thrift server side of Tachyon-Perf Master.
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final SlaveStatus mSlaveStatus;

  public MasterServiceHandler(SlaveStatus slaveStatus) {
    mSlaveStatus = slaveStatus;
  }

  @Override
  public boolean slave_canRun(int taskId, String nodeName) throws SlaveNotRegisterException,
      TException {
    return mSlaveStatus.allReady(taskId + "@" + nodeName);
  }

  @Override
  public void slave_finish(int taskId, String nodeName, boolean successFinish)
      throws SlaveNotRegisterException, TException {
    mSlaveStatus.slaveFinish(taskId + "@" + nodeName, successFinish);
  }

  @Override
  public void slave_ready(int taskId, String nodeName, boolean successSetup)
      throws SlaveNotRegisterException, TException {
    mSlaveStatus.slaveReady(taskId + "@" + nodeName, successSetup);
  }

  @Override
  public boolean slave_register(int taskId, String nodeName, String cleanupDir)
      throws SlaveAlreadyRegisterException, TException {
    return mSlaveStatus.slaveRegister(taskId + "@" + nodeName, cleanupDir);
  }
}
