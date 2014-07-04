package tachyon.web;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import tachyon.master.MasterInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileDoesNotExistException;

public class WebInterfaceDependencyServlet extends HttpServlet {
  private static final long serialVersionUID = 2071462168900313417L;
  private MasterInfo mMasterInfo;

  public WebInterfaceDependencyServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("filePath", request.getParameter("filePath"));
    request.setAttribute("error", "");
    int dependencyId = Integer.parseInt(request.getParameter("id"));
    List<String> parentFileNames = new ArrayList<String>();
    List<String> childrenFileNames = new ArrayList<String>();
    try {
      ClientDependencyInfo dependencyInfo = mMasterInfo.getClientDependencyInfo(dependencyId);
      for (int pId : dependencyInfo.parents) {
        parentFileNames.add(mMasterInfo.getPath(pId));
      }
      for (int cId : dependencyInfo.children) {
        childrenFileNames.add(mMasterInfo.getPath(cId));
      }
    } catch (DependencyDoesNotExistException ddnee) {
      request.setAttribute("error", ddnee.getMessage());
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("error", fdne.getMessage());
    }
    Collections.sort(parentFileNames);
    Collections.sort(childrenFileNames);
    request.setAttribute("parentFileNames", parentFileNames);
    request.setAttribute("childrenFileNames", childrenFileNames);
    getServletContext().getRequestDispatcher("/dependency.jsp").forward(request, response);
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    return;
  }
}