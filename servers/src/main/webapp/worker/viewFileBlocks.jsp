<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>

<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
<link href="css/tachyoncustom.min.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
  <script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
    <script src="js/bootstrap.min.js"></script>
    <script>
      function time_ago(time) {
        if (time == -1) {
          return "Unknown";
        }
        var time_formats = [
          [60, 'seconds', 1], // 60
          [120, '1 minute ago', '1 minute from now'], // 60*2
          [3600, 'minutes', 60], // 60*60, 60
          [7200, '1 hour ago', '1 hour from now'], // 60*60*2
          [86400, 'hours', 3600], // 60*60*24, 60*60
          [172800, 'Yesterday', 'Tomorrow'], // 60*60*24*2
          [604800, 'days', 86400], // 60*60*24*7, 60*60*24
          [1209600, 'Last week', 'Next week'], // 60*60*24*7*4*2
          [2419200, 'weeks', 604800], // 60*60*24*7*4, 60*60*24*7
          [4838400, 'Last month', 'Next month'], // 60*60*24*7*4*2
          [29030400, 'months', 2419200], // 60*60*24*7*4*12, 60*60*24*7*4
          [58060800, 'Last year', 'Next year'], // 60*60*24*7*4*12*2
          [2903040000, 'years', 29030400], // 60*60*24*7*4*12*100, 60*60*24*7*4*12
          [5806080000, 'Last century', 'Next century'], // 60*60*24*7*4*12*100*2
          [58060800000, 'centuries', 2903040000] // 60*60*24*7*4*12*100*20, 60*60*24*7*4*12*100
        ];
        var seconds = (+new Date() - time) / 1000,
            token = 'ago', list_choice = 1;
        if (seconds == 0) {
            return 'Just now'
        }
        if (seconds < 0) {
            seconds = Math.abs(seconds);
            token = 'from now';
            list_choice = 2;
        }
        var i = 0, format;
        while (format = time_formats[i++])
            if (seconds < format[0]) {
                if (typeof format[2] == 'string')
                    return format[list_choice];
                else
                    return Math.floor(seconds / format[2]) + ' ' + format[1] + ' ' + token;
            }
        return time;
      }
     $(document).ready(function(){
      $("abbr").each(function( index ) {
        var time = Number($(this).text());
        $(this).text(time_ago(time));
        $(this).attr("title", new Date(time));
      });
    });
    </script>
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <div class="container-fluid">
    <jsp:include page="/header" />

    <div class="container-fluid">
      <div class="row-fluid">
        <div class="span12">
          <h1 class="text-error">
            <%= request.getAttribute("fatalError") %>
          </h1>
          <h4><%= escapeHtml(request.getAttribute("path").toString()) %>
          <hr />
        </div>
      </div>

      <% if (request.getAttribute("fileBlocksOnTier") != null) { %>
        <div>
          <h5>Blocks on this worker (block capacity is <%= request.getAttribute("blockSizeBytes") %> Bytes):</h5>
          <table class="table table-bordered table-striped">
            <tr>
              <th>ID</th>
              <th>Tier</th>
              <th>Size (Byte)</th>
            </tr>
            <% for (Map.Entry<String, List<UIFileBlockInfo>> entry : ((Map<String, List<UIFileBlockInfo>>) request.getAttribute("fileBlocksOnTier")).entrySet()) { %>
              <% for (UIFileBlockInfo masterBlockInfo : entry.getValue()) { %>
                <tr>
                  <td><%= masterBlockInfo.getID() %></td>
                  <td><%= entry.getKey() %></td>
                  <td><%= masterBlockInfo.getBlockLength() %></td>
                </tr>
              <% } %>
            <% } %>
          </table>
        </div>
      <% } %>
    </div>

    <%@ include file="../footer.jsp" %>
  </div>
</body>
</html>
