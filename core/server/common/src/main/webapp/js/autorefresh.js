var requestUrl = window.location.href;
var autoRefresh = requestUrl.indexOf("autoRefresh=") != -1;

function doAutoRefresh() {
  var refreshUrl = window.location.href;
  refreshUrl = refreshUrl.replace(window.location.hash, "");
  if (refreshUrl.indexOf("?") == -1) {
    refreshUrl = refreshUrl + "?";
  }
  refreshUrl = refreshUrl.replace(/(.+)&?autoRefresh=([^&]+)(.+)?/, "$1$3");
  refreshUrl = refreshUrl + "&autoRefresh=1";
  refreshUrl = refreshUrl.replace("?&", "?");
  window.location.replace(refreshUrl);
}

var autoRefreshTimer;
function toggleAutoRefresh() {
  if (autoRefresh) {
    autoRefresh = false;
    window.clearTimeout(autoRefreshTimer);
    $("#autorefresh-link").text("Enable Auto-Refresh");
  } else {
    autoRefresh = true;
    autoRefreshTimer = window.setTimeout(doAutoRefresh, 15000);
    $("#autorefresh-link").text("Disable Auto-Refresh");
  }
}
if (autoRefresh) {
  $("#autorefresh-link").text("Disable Auto-Refresh");
  autoRefreshTimer = window.setTimeout(doAutoRefresh, 15000);
} else {
  $("#autorefresh-link").text("Enable Auto-Refresh");
}
