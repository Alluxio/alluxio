package alluxio.stress.cli.journalTool;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.journal.raft.JournalStateMachine;
import alluxio.master.journal.raft.RaftJournal;
import alluxio.master.journal.raft.RaftJournalAppender;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.master.journal.raft.RaftJournalWriter;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalLogWriter;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.TimeDuration;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class JournalExporter {
  static String mOutputDir = "/Users/dengxinyu/journal-tool/";
  protected final String mInputDir;
  protected final String mMaster;
  protected final long mStart;
  public JournalWriter mJournalWriter;
  public Journal mJournal;

  private RaftGroup mRaftGroup;

  public JournalExporter(JournalType journalType, String inputDir, String master, long start) throws IOException {
    mInputDir = inputDir;
    mMaster = master;
    mStart = start;

    switch (journalType) {
      case UFS:
        System.out.println("ufs");
        initUfsJournal();
        break;
      case EMBEDDED:
        System.out.println("raft");
        initRaftJournal();
        break;
      default:
        throw new RuntimeException("Unknown Journal Type");
    }
  }

  public JournalWriter getWriter() {
    return mJournalWriter;
  }

  public Journal getJournal() {
    return mJournal;
  }

  private void initUfsJournal() throws IOException {
    // UfsJournal journal = new UfsJournalSystem(getJournalLocation(mInputDir), 0).createJournal(new NoopMaster(mMaster));
    System.out.println("ufsing");
    UfsJournal journal = new UfsJournal(getJournalLocation(mOutputDir), new NoopMaster(), 0, Collections::emptySet);
    mJournal = journal;
    journal.start();
    journal.suspend();
    journal.gainPrimacy();
    JournalWriter writer = new UfsJournalLogWriter(journal, mStart);
    mJournalWriter = writer;
  }

  private void initRaftJournal() {
    try {
      RaftJournalSystem sys = new RaftJournalSystem(new URI("/Users/dengxinyu/journal-tool/raft"), NetworkAddressUtils.ServiceType.MASTER_RAFT);
      mJournal = sys.createJournal(new NoopMaster());
      System.out.println("1");
      sys.start();
      System.out.println("2");
      sys.gainPrimacy();
      System.out.println("3");
      Class<?> clazz = RaftJournalSystem.class;
      System.out.println("creating clazz");
      System.out.println(clazz);
      Field writer = clazz.getDeclaredField("mRaftJournalWriter");
      System.out.println("getting mRaftJournalWriter");
      System.out.println(writer);
      writer.setAccessible(true);
      System.out.println("setting mRaftJournalWriter accessible");
      System.out.println(writer);
      Object raftJournalWriter = writer.get(sys);
      System.out.println("Value of private writer: " + raftJournalWriter);
      if (raftJournalWriter == null) {
        System.out.println("raftJournalWriter is NULL!!!");
      }
      mJournalWriter = (JournalWriter) raftJournalWriter;
      System.out.println("ok when initiating raft writer");
    } catch (Exception e) {
      // do sth
      System.out.println("failed when initiating raft writer");
      System.out.println(e);
    }
  }

  // private void initRaftJournal() throws IOException {
  //   final UUID RAFT_GROUP_UUID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
  //   RaftGroupId RAFT_GROUP_ID = RaftGroupId.valueOf(RAFT_GROUP_UUID);
  //   List<InetSocketAddress> mClusterAddresses = ConfigurationUtils.getEmbeddedJournalAddresses(Configuration.global(), NetworkAddressUtils // .ServiceType.MASTER_RAFT);
  //   ConcurrentHashMap<String, RaftJournal> mJournals = new ConcurrentHashMap<>();
  //   JournalStateMachine mStateMachine;
  //   Set<RaftPeer> peers = mClusterAddresses.stream()
  //     .map(addr -> RaftPeer.newBuilder()
  //               .setId(RaftJournalUtils.getPeerId(addr))
  //               .setAddress(addr)
  //               .build()
  //       )
  //       .collect(Collectors.toSet());
  //   mRaftGroup = RaftGroup.valueOf(RAFT_GROUP_ID, peers);
  //   mStateMachine = new JournalStateMachine(mJournals, new RaftJournalSystem(new URI(""), NetworkAddressUtils.ServiceType.MASTER_RAFT));
  //   RaftServer mServer = RaftServer.newBuilder()
  //       .setServerId(RaftJournalUtils.getPeerId(NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RAFT, // Configuration.global())))
  //       .setGroup(mRaftGroup)
  //       .setStateMachine(mStateMachine)
  //       .setProperties(properties)
  //       .setParameters(parameters)
  //       .build();
  //   RaftJournalAppender client = new RaftJournalAppender(mServer, this::createClient)
  //   UfsJournal journal = new UfsJournalSystem(getJournalLocation(mInputDir), 0).createJournal(new NoopMaster(mMaster));
  //   JournalWriter reader = new UfsJournalLogWriter(journal, mStart);
  // }

  // private RaftClient createClient() {
  //   return createClient(Configuration.getMs(
  //       PropertyKey.MASTER_EMBEDDED_JOURNAL_RAFT_CLIENT_REQUEST_TIMEOUT));
  // }

  private RaftClient createClient(long timeoutMs) {
    long retryBaseMs =
        Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_RAFT_CLIENT_REQUEST_INTERVAL);
    long maxSleepTimeMs =
        Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT);
    RaftProperties properties = new RaftProperties();
    Parameters parameters = new Parameters();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(timeoutMs, TimeUnit.MILLISECONDS));
    RetryPolicy retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(TimeDuration.valueOf(retryBaseMs, TimeUnit.MILLISECONDS))
        .setMaxSleepTime(TimeDuration.valueOf(maxSleepTimeMs, TimeUnit.MILLISECONDS))
        .build();
    return RaftClient.newBuilder()
        .setRaftGroup(mRaftGroup)
        .setClientId(ClientId.randomId())
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(parameters)
        .setRetryPolicy(retryPolicy)
        .build();
  }

 private URI getJournalLocation(String inputDir) {
  if (!inputDir.endsWith(AlluxioURI.SEPARATOR)) {
    inputDir += AlluxioURI.SEPARATOR;
  }
  try {
    return new URI(inputDir);
  } catch (URISyntaxException e) {
    throw new RuntimeException(e);
  }
}
}
