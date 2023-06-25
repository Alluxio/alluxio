package alluxio.worker.http;

public class ResponseFileInfo {

  private final String mType;

  private final String mName;

  public ResponseFileInfo(String type, String name) {
    this.mType = type;
    this.mName = name;
  }

  public String getType() {
    return mType;
  }

  public String getName() {
    return mName;
  }
}
