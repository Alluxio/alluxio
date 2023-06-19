package alluxio.membership;

public interface StateListener {
  public void onNewPut(String newPutKey, byte[] newPutValue);
  public void onNewDelete(String newDeleteKey);
}
