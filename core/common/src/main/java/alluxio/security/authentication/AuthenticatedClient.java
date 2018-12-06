package alluxio.security.authentication;

import javax.security.sasl.SaslServer;

public class AuthenticatedClient {
  private SaslServer mSaslServer;
  private String mUserName;

  public SaslServer getSaslServer() {
    return mSaslServer;
  }

  public void setSaslServer(SaslServer mSaslServer) {
    this.mSaslServer = mSaslServer;
  }

  public String getUserName() {
    return mUserName;
  }

  public void setUserName(String mUserName) {
    this.mUserName = mUserName;
  }
}
