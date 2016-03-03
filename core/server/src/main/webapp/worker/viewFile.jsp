<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>
<%@ page import="static java.net.URLEncoder.encode" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="../css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="../css/custom.min.css" rel="stylesheet">
  <link href="../img/favicon.ico" rel="shortcut icon">
</head>
<title>Alluxio</title>
<body>
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <jsp:include page="../viewFile-body.jsp" />
</body>
</html>
