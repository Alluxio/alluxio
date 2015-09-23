package tachyon.security.authentication;

/**
 * Different authentication types for Tachyon.
 */
public enum AuthType {
  /**
   * Authentication is disabled. No user info in Tachyon.
   */
  NOSASL("NOSASL"),

  /**
   * User is aware in Tachyon. Login user is OS user. The verification of client user is disabled.
   */
  SIMPLE("SIMPLE"),

  /**
   * User is aware in Tachyon. Login user is OS user. The user is verified by Custom
   * authentication provider (Use with property tachyon.authentication.provider.custom.class).
   */
  CUSTOM("CUSTOM"),

  /**
   * User is aware in Tachyon. The user is verified by Kerberos authentication. NOTE: this
   * authentication is not supported.
   */
  KERBEROS("KERBEROS");

  private final String mAuthType;

  AuthType(String authType) {
    mAuthType = authType;
  }

  public String getAuthName() {
    return mAuthType;
  }
}
