package alluxio;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit run listener which prints full thread dump into System.err in case a test is failed due to
 * timeout.
 */
public class TimedOutTestsListenerFromBookKeeper extends RunListener {

  private static final Logger LOG = LoggerFactory.getLogger(TimedOutTestsListenerFromBookKeeper.class);

  static final String TEST_TIMED_OUT_PREFIX = "test timed out after";

  private static String indent = "    ";

  public TimedOutTestsListenerFromBookKeeper() {
  }

  @Override
  public void testFailure(Failure failure) throws Exception {
    if (failure != null && failure.getMessage() != null && failure.getMessage().startsWith(TEST_TIMED_OUT_PREFIX)) {
      LOG.info("====> TEST TIMED OUT. PRINTING THREAD DUMP. <====");
      LOG.info("");
      LOG.info(buildThreadDiagnosticString());
    }
  }

  public static String buildThreadDiagnosticString() {
    StringWriter sw = new StringWriter();
    PrintWriter output = new PrintWriter(sw);

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    output.println(String.format("Timestamp: %s", dateFormat.format(new Date())));
    output.println();
    output.println(buildThreadDump());

    String deadlocksInfo = buildDeadlockInfo();
    if (deadlocksInfo != null) {
      output.println("====> DEADLOCKS DETECTED <====");
      output.println();
      output.println(deadlocksInfo);
    }

    return sw.toString();
  }

  static String buildThreadDump() {
    StringBuilder dump = new StringBuilder();
    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
      Thread thread = e.getKey();
      dump.append(String.format("\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s", thread.getName(),
          (thread.isDaemon() ? "daemon" : ""), thread.getPriority(), thread.getId(),
          Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()"
              : thread.getState().name().toLowerCase(),
          Thread.State.WAITING.equals(thread.getState()) ? "WAITING (on object monitor)" : thread.getState()));
      for (StackTraceElement stackTraceElement : e.getValue()) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n");
    }
    return dump.toString();
  }

  static String buildDeadlockInfo() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    long[] threadIds = threadBean.findMonitorDeadlockedThreads();
    if (threadIds != null && threadIds.length > 0) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter out = new PrintWriter(stringWriter);

      ThreadInfo[] infos = threadBean.getThreadInfo(threadIds, true, true);
      for (ThreadInfo ti : infos) {
        printThreadInfo(ti, out);
        printLockInfo(ti.getLockedSynchronizers(), out);
        out.println();
      }

      out.close();
      return stringWriter.toString();
    } else {
      return null;
    }
  }

  private static void printThreadInfo(ThreadInfo ti, PrintWriter out) {
    // print thread information
    printThread(ti, out);

    // print stack trace with locks
    StackTraceElement[] stacktrace = ti.getStackTrace();
    MonitorInfo[] monitors = ti.getLockedMonitors();
    for (int i = 0; i < stacktrace.length; i++) {
      StackTraceElement ste = stacktrace[i];
      out.println(indent + "at " + ste.toString());
      for (MonitorInfo mi : monitors) {
        if (mi.getLockedStackDepth() == i) {
          out.println(indent + "  - locked " + mi);
        }
      }
    }
    out.println();
  }

  private static void printThread(ThreadInfo ti, PrintWriter out) {
    out.print("\"" + ti.getThreadName() + "\"" + " Id=" + ti.getThreadId() + " in " + ti.getThreadState());
    if (ti.getLockName() != null) {
      out.print(" on lock=" + ti.getLockName());
    }
    if (ti.isSuspended()) {
      out.print(" (suspended)");
    }
    if (ti.isInNative()) {
      out.print(" (running in native)");
    }
    out.println();
    if (ti.getLockOwnerName() != null) {
      out.println(indent + " owned by " + ti.getLockOwnerName() + " Id=" + ti.getLockOwnerId());
    }
  }

  private static void printLockInfo(LockInfo[] locks, PrintWriter out) {
    out.println(indent + "Locked synchronizers: count = " + locks.length);
    for (LockInfo li : locks) {
      out.println(indent + "  - " + li);
    }
    out.println();
  }

}