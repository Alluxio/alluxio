<%@ page isELIgnored ="false" %> 
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>  

<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
</head>
<title>Tachyon</title>
<body>
<script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="js/bootstrap.min.js"></script>
<div class="container-fluid">
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills">
        <li><a href="./home">Master: ${masterNodeAddress}</a></li>
        <li class="active"><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <h1 class="text-error">
          ${invalidPathError}
        </h1>
        <div class="navbar">
          <div class="navbar-inner">
            <ul class="nav nav-pills">
              <c:forEach var="pathInfo" items="${pathInfos}">
                <li><a href="./browse?path=${pathInfo.absolutePath}"><c:out value="${pathInfo.name}"/></a></li>
              </c:forEach>
              <li class="active"><a href="./browse?path=${currentPath}"><c:out value="${currentDirectory.name}"/></a></li>
            </ul>
          </div>
        </div>
        <table class="table">
          <thead>
            <th>File Name</th>
            <th>Size</th>
            <th>In-Memory</th>
            <c:if test = "${debug}">
              <th>[DEBUG]Inode Number</th>
            </c:if>
          </thead>
          <tbody>
            <c:forEach var="fileInfo" items="${fileInfos}">
              <tr>
                <th>
                  <c:if test = "${fileInfo.isDirectory}">
                    <i class="icon-folder-close"></i>
                  </c:if>
                  <c:if test = "${not fileInfo.isDirectory}">
                    <i class="icon-file"></i>
                  </c:if>
                  <a href="./browse?path=${fileInfo.absolutePath}"><c:out value="${fileInfo.name}"/></a>
                </th>
                <th>${fileInfo.size} Bytes</th>
                <th>
                  <c:if test = "${fileInfo.inMemory}">
                    <i class="icon-hdd"></i>
                  </c:if>
                  <c:if test = "${not fileInfo.inMemory}">
                    <i class="icon-hdd icon-white"></i>
                  </c:if>
                </th>
                <c:if test = "${debug}">
                  <th>${fileInfo.id}</th>
                </c:if>
              </tr>
            </c:forEach>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <footer>
    <p>Tachyon is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.</p>
  </footer>
</div>
</body>
</html>