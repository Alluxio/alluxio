package alluxio.master;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.heartbeat.HeartbeatExecutor;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public final class RatisHealthChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RatisHealthChecker.class);

  private RaftServer mServer;
  private final FaultTolerantAlluxioMasterProcess mMaster;

  private int errorCount = 0;
  private RaftGroupId mGroupId;

  public RatisHealthChecker(RaftServer server, FaultTolerantAlluxioMasterProcess master) {
    mServer = server;
    mMaster = master;
  }

  public void setGroupId(RaftGroupId id) {
    mGroupId = id;
    LOG.info("Updated RaftGroupId to {}", mGroupId);
  }
  public void setRaftServer(RaftServer server){
    mServer = server;
    LOG.info("Updated RaftServer to {}", mServer.getId());
  }

  @Override
  public void heartbeat() {
    if (mServer == null) {
      LOG.info("Ratis server not up yet");
      return;
    }
    boolean restart = false;
    LifeCycle.State state = mServer.getLifeCycleState();
    LOG.info("Server state {}", state);
    // TODO(jiacheng): extract this to a method
    if (!isHealthy()) {
      errorCount++;
      if (errorCount >= 60) {
        LOG.error("Raft server has been not serving for more than 60s. Attempt to format and restart");
        restart = true;
      }
    } else {
      if (errorCount != 0) {
        // The server was in a bad state, now it has recovered somehow
        LOG.info("Ratis server has recovered from a bad state. Reset the health checker.");
        errorCount = 0;
      }
    }

    if (restart) {
      LOG.warn("Trying to restart the Raft Server now");
      Instant startTime = Instant.now();
      mMaster.restart();
      Instant endTime = Instant.now();
      Duration elapsedTime = Duration.between(startTime, endTime);
      LOG.info("The master has restarted from a fresh journal. The restart took {}ms.", elapsedTime.toMillis());
    }
  }

  private boolean isHealthy() {
    LifeCycle.State state = mServer.getLifeCycleState();
    LOG.info("Server state {}", state);
    // TODO(jiacheng): extract this to a method
    if (state != LifeCycle.State.RUNNING) {
      LOG.warn("Ratis server is in state {}", state);
      if (state == LifeCycle.State.CLOSING || state == LifeCycle.State.CLOSED || state == LifeCycle.State.EXCEPTION) {
        return false;
      }
    }
    // Check the ratis group if available
//    if (mGroupId != null) {
//      try {
//        RaftServer.Division d = mServer.getDivision(mGroupId);
//        Field f = d.getClass().getDeclaredField("state");
//        f.setAccessible(true);
//        f.get(d);
//
//      } catch (Exception e) {
//
//      }
//    }

    return true;
  }

  @Override
  public void close() {
    // Nothing to clean up.
  }
}