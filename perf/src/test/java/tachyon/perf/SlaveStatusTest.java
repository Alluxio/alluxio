package tachyon.perf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.perf.thrift.SlaveAlreadyRegisterException;
import tachyon.perf.thrift.SlaveNotRegisterException;

public class SlaveStatusTest {
  private Set<String> mSlaves;

  @Before
  public final void before() throws IOException {
    mSlaves = new HashSet<String>();
    mSlaves.add("xxx");
    mSlaves.add("yyy");
    mSlaves.add("zzz");
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void allReadyNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.allReady("xxx");
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void slaveFinishNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.slaveFinish("yyy", true);
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void slaveReadyNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.slaveReady("zzz", true);
  }

  @Test(expected = SlaveAlreadyRegisterException.class)
  public void slaveRegisterAlreadyRegisterTest() throws SlaveAlreadyRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    ss.slaveRegister("xxx", null);
  }

  @Test
  public void allRegisteredTest() throws SlaveAlreadyRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    Assert.assertTrue(ss.slaveRegister("yyy", null));
    Assert.assertTrue(ss.slaveRegister("zzz", null));
    Assert.assertTrue(ss.allRegistered());
  }

  @Test
  public void finishedTest() throws SlaveAlreadyRegisterException, SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    Assert.assertTrue(ss.slaveRegister("yyy", null));
    Assert.assertTrue(ss.slaveRegister("zzz", null));
    ss.slaveReady("xxx", true);
    ss.slaveReady("yyy", false);
    ss.slaveReady("zzz", true);
    Assert.assertEquals(0, ss.finished(false, 0));
    Assert.assertEquals(-1, ss.finished(true, 0));
    Assert.assertEquals(0, ss.finished(true, 100));
    ss.slaveFinish("xxx", true);
    ss.slaveFinish("yyy", true);
    ss.slaveFinish("zzz", false);
    Assert.assertEquals(1, ss.finished(false, 0));
    Assert.assertEquals(-1, ss.finished(true, 0));
    Assert.assertEquals(-1, ss.finished(true, 50));
    Assert.assertEquals(1, ss.finished(true, 100));
  }
}
