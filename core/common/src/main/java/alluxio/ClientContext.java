package alluxio;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

public class ClientContext {

  public final AlluxioConfiguration mConf;
  public final Subject mSubject;

  /**
   * A client context with information about the subject and configuration of the client.
   *
   * This class accepts {@link AlluxioProperties} instead of {@link AlluxioConfiguration} because
   * AlluxioProperties is a mutable object that should be able to be created and modified easily by
   * a user. Meanwhile, the {@link AlluxioConfiguration} interface is read only and doesn't allow
   * the user to change values once instantiated. The ClientContext class is designed to be
   * immutable. This forces any changes to a configuration to initialize a new Context.
   *
   * @param subject The security subject to use
   * @param props The AlluxioProperties to use. If null,
   * @return A new client context with the specified properties and subject
   */
  public static ClientContext create(@Nullable Subject subject, @Nullable AlluxioProperties props) {
    return new ClientContext(subject, props);
  }

  /**
   * @param props The specified {@link AlluxioProperties} to use.
   * @return
   */
  public static ClientContext create(@Nullable AlluxioProperties props) {
    return new ClientContext(null, props);
  }

  /**
   * @return a new {@link ClientContext} with values loaded from the alluxio-site properties and a
   * null subject
   */
  public static ClientContext create() {
    return new ClientContext(null, null);
  }

  private ClientContext(@Nullable Subject subject, @Nullable AlluxioProperties props) {
    mSubject = subject;
    // Copy the properties so that future modification doesn't affect this ClientContext.
    if (props != null) {
      mConf = new InstancedConfiguration(props.copy());
    } else {
      mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    }

  }

  public AlluxioConfiguration getConfiguration() {
    return mConf;
  }

  public Subject getSubject() {
    return mSubject;
  }

}
