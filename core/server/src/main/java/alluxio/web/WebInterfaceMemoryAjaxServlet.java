/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.AlluxioMasterService;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;
import alluxio.web.entity.FilterEntity;
import alluxio.web.entity.PageResultEntity;
import alluxio.web.entity.PaginationOptionsEntity;
import alluxio.web.entity.SortEntity;
import alluxio.wire.FileInfo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Servlet that provides ajax request for browsing the file system in memory fully.
 */
@ThreadSafe
public final class WebInterfaceMemoryAjaxServlet extends HttpServlet {
  private static final transient Map<String, Method> FILE_INFO_GETTER_MAP = new HashMap<>();
  private static final transient Map<String, String> FILE_INFO_SPECIAL_FIELD_MAP = new HashMap<>();
  private static final long serialVersionUID = -8262727381905167368L;
  private final transient AlluxioMasterService mMaster;

  static {
    Method[] methods = UIFileInfo.class.getMethods();
    for (Method method : methods) {
      //getter must start with get, no parameter and return type mustn't be void
      if (method.getName().startsWith("get") && method.getParameterTypes().length == 0
          && !void.class.equals(method.getReturnType())) {
        String key = StringUtils.uncapitalize(method.getName().substring("get".length()));
        FILE_INFO_GETTER_MAP.put(key, method);
      } else if (method.getName().startsWith("is") && method.getParameterTypes().length == 0
          && boolean.class.equals(method.getReturnType())) {
        String key = StringUtils.uncapitalize(method.getName().substring("is".length()));
        FILE_INFO_GETTER_MAP.put(key, method);
      }
    }
    FILE_INFO_SPECIAL_FIELD_MAP.put("creationTime", "mCreationTimeMs");
    FILE_INFO_SPECIAL_FIELD_MAP.put("modificationTime", "mLastModificationTimeMs");
  }

  /**
   * Creates a new instance of {@link WebInterfaceMemoryAjaxServlet}.
   *
   * @param master the Alluxio master
   */
  public WebInterfaceMemoryAjaxServlet(AlluxioMasterService master) {
    mMaster = master;
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws
      ServletException, IOException {
    doGet(req, resp);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request  the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException      if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    PageResultEntity pageResultEntity = new PageResultEntity();
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    pageResultEntity.getArgumentMap().put("masterNodeAddress", mMaster.getRpcAddress().toString());
    pageResultEntity.getArgumentMap().put("fatalError", "");
    pageResultEntity.getArgumentMap().put("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

    List<AlluxioURI> inMemoryFiles = mMaster.getFileSystemMaster().getInMemoryFiles();
    // Collections.sort(inMemoryFiles);

    String paginationOptions = request.getParameter("paginationOptions");
    final PaginationOptionsEntity paginationOptionsEntity = JSON.parseObject(paginationOptions,
        PaginationOptionsEntity.class);
    if (paginationOptionsEntity == null) {
      return;
    }
    //get all fullInMemory fileInfo;
    List<UIFileInfo> fileInfos = new ArrayList<>();
    for (AlluxioURI file : inMemoryFiles) {
      try {
        long fileId = mMaster.getFileSystemMaster().getFileId(file);
        FileInfo fileInfo = mMaster.getFileSystemMaster().getFileInfo(fileId);
        if (fileInfo != null && fileInfo.getInMemoryPercentage() == 100) {
          fileInfos.add(new UIFileInfo(fileInfo));
        }
      } catch (FileDoesNotExistException e) {
        pageResultEntity.getArgumentMap().put("fatalError", "Error: File does not exist "
            + e.getLocalizedMessage());
        response.getWriter().write(JSON.toJSONString(pageResultEntity));
        return;
      } catch (AccessControlException e) {
        pageResultEntity.getArgumentMap().put("permissionError",
            "Error: File " + file + " cannot be accessed " + e.getMessage());
        response.getWriter().write(JSON.toJSONString(pageResultEntity));
        return;
      }
    }
    request.setAttribute("inMemoryFileNum", fileInfos.size());

    //sort
    if (paginationOptionsEntity.getSorters() != null
        && paginationOptionsEntity.getSorters().size() > 0) {
      Collections.sort(fileInfos, new Comparator<UIFileInfo>() {
        @Override
        public int compare(UIFileInfo o1, UIFileInfo o2) {
          try {
            for (SortEntity sortEntity : paginationOptionsEntity.getSorters()) {
              int sortDirection = SortEntity.SORT_ASC.equals(sortEntity.getDirection()) ? 1 : -1;
              if (FILE_INFO_SPECIAL_FIELD_MAP.containsKey(sortEntity.getField())) {
                Field field = UIFileInfo.class.getDeclaredField(
                    FILE_INFO_SPECIAL_FIELD_MAP.get(sortEntity.getField()));
                field.setAccessible(true);
                Long o1Value = (Long) field.get(o1);
                Long o2Value = (Long) field.get(o2);
                int compareResult = o1Value.compareTo(o2Value);
                if (compareResult != 0) {
                  return compareResult * sortDirection;
                }
              } else {
                Method getterMethod = FILE_INFO_GETTER_MAP.get(sortEntity.getField());
                if (getterMethod != null) {
                  Object o1Value = getterMethod.invoke(o1);
                  Object o2Value = getterMethod.invoke(o2);
                  if (o1Value instanceof Comparable) {
                    int compareResult = ((Comparable) o1Value).compareTo(o2Value);
                    if (compareResult != 0) {
                      return compareResult * sortDirection;
                    }
                  } else {
                    return 0;
                  }
                }
              }
            }
          } catch (NoSuchFieldException e) {
            e.printStackTrace();
          } catch (IllegalAccessException e) {
            e.printStackTrace();
          } catch (InvocationTargetException e) {
            e.printStackTrace();
          }
          return 0;
        }
      });
    }
    //filter
    List<FilterEntity> filterEntityList = paginationOptionsEntity.getFilters();
    if (filterEntityList != null && filterEntityList.size() > 0) {

      Iterator<UIFileInfo> uiFileInfoIter = fileInfos.iterator();
      while (uiFileInfoIter.hasNext()) {
        boolean filterPassed = true;
        UIFileInfo uiFileInfo = uiFileInfoIter.next();
        for (FilterEntity fe : filterEntityList) {
          if (FILE_INFO_GETTER_MAP.containsKey(fe.getField())) {
            Method getterMethod = FILE_INFO_GETTER_MAP.get(fe.getField());
            Object value = null;
            try {
              value = getterMethod.invoke(uiFileInfo);
              if (value instanceof String) {
                if (!((String) value).contains(fe.getTerm())) {
                  filterPassed = false;
                }
              } else if (value instanceof Integer) {
                if (!value.equals(Integer.parseInt(fe.getTerm()))) {
                  filterPassed = false;
                }
              } else if (value instanceof Boolean) {
                if (!value.equals(Boolean.parseBoolean(fe.getTerm()))) {
                  filterPassed = false;
                }
              }
            } catch (IllegalAccessException e) {
              e.printStackTrace();
            } catch (InvocationTargetException e) {
              e.printStackTrace();
            }

          }
        }
        if (!filterPassed) {
          uiFileInfoIter.remove();
        }
      }
    }
    int offset = (paginationOptionsEntity.getPageNumber() - 1) * paginationOptionsEntity
        .getPageSize();
    if (offset >= fileInfos.size()) {
      offset = 0;
    }
    int length = (offset + paginationOptionsEntity.getPageSize()) > fileInfos.size()
        ? fileInfos.size() : offset + paginationOptionsEntity.getPageSize();

    List<UIFileInfo> subFileInfo = fileInfos.subList(offset, length);

    pageResultEntity.setPageData(subFileInfo);
    pageResultEntity.setTotalCount(fileInfos.size());
    String json = JSON.toJSONString(pageResultEntity);

    response.getWriter().write(json);
  }
}
