package alluxio;

import org.apache.log4j.MDC;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

public class AlluxioRemoteLogFilter extends Filter {
  public static final String APPENDER_NAME_OPTION = "AppenderName";

  String mAppenderName;

  public String[] getAppenderStrings() {
    return new String[] {APPENDER_NAME_OPTION};
  }

  public void setOption(String key, String value) {
    if (key.equalsIgnoreCase("AppenderName")) {
      mAppenderName = value;
    }
  }

  public void setAppenderName(String appenderName) {
    mAppenderName = appenderName;
  }

  public String getAppenderName() {
    return mAppenderName;
  }

  @Override
  public int decide(LoggingEvent event) {
    MDC.put("appender", mAppenderName);
    return ACCEPT;
  }
}
