package tachyon;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;

import tachyon.thrift.CoordinatorService;

public class CoordinatorServiceHandler implements CoordinatorService.Iface{
  private Journal mJournal;

  public CoordinatorServiceHandler(Journal journal) {
    mJournal = journal;
  }

  @Override
  public long getMaxTransactionId() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long sendNewTransactions(long leftTransactionId, long rightTransactionId,
      List<ByteBuffer> transactions) throws TException {
    // TODO Auto-generated method stub
    return 0;
  }
}
