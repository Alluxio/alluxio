package tachyon.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * Servlet that provides data for displaying which files are currently in memory.
 */
public class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private static final int ITEMS_PER_PAGE = 20;
  // number of pages shown in the pagination UI component
  private static final int PAGINATION_SHOW = 5;
  private int mCurrentPageNum;
  private int mFirstPageNum;
  private int mLastPageNum;
  private MasterInfo mMasterInfo;

  public WebInterfaceMemoryServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Populates attributes before redirecting to a jsp.
   * 
   * @param request
   *          The HttpServletRequest object
   * @param response
   *          The HttpServletReponse object
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());

    List<String> inMemoryFiles = mMasterInfo.getInMemoryFiles();
    Collections.sort(inMemoryFiles);
    request.setAttribute("inMemoryFiles", inMemoryFiles);

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(inMemoryFiles.size());
    for (String file : inMemoryFiles) {
      try {
        ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo(file);
        if (fileInfo != null && fileInfo.getInMemoryPercentage() == 100) {
          fileInfos.add(new UiFileInfo(fileInfo));
        }
      } catch (InvalidPathException ipe) {
        request.setAttribute("invalidPathError",
            "Error: Invalid Path " + ipe.getLocalizedMessage());
        getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
        return;
      }
    }
    final int ITEMS_PER_PAGE = 20;
    int pageNum;
    try {
      pageNum = Integer.parseInt(request.getParameter("pageNum"));
    } catch (NumberFormatException nfe) {
      pageNum = 1;
    }
    int from = Math.min(fileInfos.size(), (pageNum - 1) * ITEMS_PER_PAGE);
    int to = Math.min(fileInfos.size(), pageNum * ITEMS_PER_PAGE);
    request.setAttribute("fileInfos", fileInfos.subList(from, to));

    updatePaginationValues(fileInfos, pageNum);
    populatePaginationValues(request);

    getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
  }

  private void updatePaginationValues(List<UiFileInfo> fileInfos, int pageNum) {
    mCurrentPageNum = pageNum;
    mFirstPageNum = mCurrentPageNum - (mCurrentPageNum - 1) % PAGINATION_SHOW;
    if (fileInfos.size() == 0) {
      mLastPageNum = 0;
    } else {
      mLastPageNum = (fileInfos.size() - 1) / ITEMS_PER_PAGE + 1;
    }
  }

  private void populatePaginationValues(HttpServletRequest request) {
    request.setAttribute("paginationShow", new Integer(PAGINATION_SHOW));
    request.setAttribute("currentPageNum", new Integer(mCurrentPageNum));
    request.setAttribute("firstPageNum", new Integer(mFirstPageNum));
    request.setAttribute("lastPageNum", new Integer(mLastPageNum));
  }
}