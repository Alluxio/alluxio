package alluxio.stress.cli.journaldisruptor;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalLogWriter;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.ratis.protocol.RaftGroup;

import java.lang.reflect.Field;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;


public class JournalExporter {
  static String mOutputDir = "/Users/dengxinyu/journal-tool/";
  protected final String mInputDir;
  protected final String mMaster;
  protected final long mStart;
  public JournalWriter mJournalWriter;
  public Journal mJournal;

  private RaftGroup mRaftGroup;

  public JournalExporter(String inputDir, String master, long start) throws IOException {
    JournalType journalType = Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
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
      sys.start();
      sys.gainPrimacy();
      Class<?> clazz = RaftJournalSystem.class;
      Field writer = clazz.getDeclaredField("mRaftJournalWriter");
      writer.setAccessible(true);
      Object raftJournalWriter = writer.get(sys);
      mJournalWriter = (JournalWriter) raftJournalWriter;
    } catch (Exception e) {
      // do sth
      System.out.println("failed when initiating raft writer");
      System.out.println(e);
    }
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
