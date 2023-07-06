package alluxio.membership;

import alluxio.wire.WorkerInfo;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * No op membership manager for testing purpose.
 */
public class NoOpMembershipManager implements MembershipManager {
  @Override
  public void join(WorkerInfo worker) throws IOException {
    // NO-OP
  }

  @Override
  public List<WorkerInfo> getAllMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<WorkerInfo> getLiveMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<WorkerInfo> getFailedMembers() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public String showAllMembers() {
    return StringUtils.EMPTY;
  }

  @Override
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    // NO OP
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    // NO OP
  }

  @Override
  public void close() throws Exception {
    // NO OP
  }
}
