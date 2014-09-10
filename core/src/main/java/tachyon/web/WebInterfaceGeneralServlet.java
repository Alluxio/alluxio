package tachyon.web;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.Version;
import tachyon.master.DependencyVariables;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for viewing the general status of the filesystem.
 */
public class WebInterfaceGeneralServlet extends HttpServlet {
  private static final long serialVersionUID = 2335205655766736309L;

  private MasterInfo mMasterInfo;

  public WebInterfaceGeneralServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   * 
   * @param request
   *          The HttpServletRequest object
   * @param response
   *          The HttpServletResponse object
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    DependencyVariables.VARIABLES.clear();
    for (String key : (Set<String>) request.getParameterMap().keySet()) {
      if (key.startsWith("varName")) {
        String value = request.getParameter("varVal" + key.substring(7));
        if (value != null) {
          DependencyVariables.VARIABLES.put(request.getParameter(key), value);
        }
      }
    }
    populateValues(request);
    getServletContext().getRequestDispatcher("/general.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display
   * 
   * @param request
   *          The HttpServletRequest object
   * @throws IOException
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());

    request.setAttribute("uptime", CommonUtils.convertMsToClockTime(System.currentTimeMillis()
        - mMasterInfo.getStarttimeMs()));

    request.setAttribute("startTime", CommonUtils.convertMsToDate(mMasterInfo.getStarttimeMs()));

    request.setAttribute("version", Version.VERSION);

    request.setAttribute("liveWorkerNodes", Integer.toString(mMasterInfo.getWorkerCount()));

    request.setAttribute("capacity", CommonUtils.getSizeFromBytes(mMasterInfo.getCapacityBytes()));

    request.setAttribute("usedCapacity", CommonUtils.getSizeFromBytes(mMasterInfo.getUsedBytes()));

    request.setAttribute("freeCapacity", CommonUtils.getSizeFromBytes((mMasterInfo
        .getCapacityBytes() - mMasterInfo.getUsedBytes())));

    long sizeBytes = mMasterInfo.getUnderFsCapacityBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskCapacity", "UNKNOWN");
    }

    sizeBytes = mMasterInfo.getUnderFsUsedBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskUsedCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskUsedCapacity", "UNKNOWN");
    }

    sizeBytes = mMasterInfo.getUnderFsFreeBytes();
    if (sizeBytes >= 0) {
      request.setAttribute("diskFreeCapacity", CommonUtils.getSizeFromBytes(sizeBytes));
    } else {
      request.setAttribute("diskFreeCapacity", "UNKNOWN");
    }

    request.setAttribute("recomputeVariables", DependencyVariables.VARIABLES);
  }
}
