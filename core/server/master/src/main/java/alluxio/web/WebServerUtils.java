package alluxio.web;

import alluxio.master.PrimarySelector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class WebServerUtils {
  public static void addRedirectionFilter(ServletContextHandler contextHandler, PrimarySelector primarySelector) {
    // Add filter for authenticating users.
    RedirectionFilter filter = new RedirectionFilter(primarySelector);
    contextHandler.addFilter(new FilterHolder(filter), "/*",
            EnumSet.of(javax.servlet.DispatcherType.REQUEST,
                    javax.servlet.DispatcherType.FORWARD, javax.servlet.DispatcherType.INCLUDE));
  }
}
