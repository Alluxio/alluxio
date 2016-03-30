<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

  // default current states
  var currentPage = 1;
  var currentOffset = 0;
  var currentLimit = initialLimit();

  function initialLimit() {
    return Math.min(nFilePerPage, nTotalFile);
  }
 
  function redirect() {
    var url = window.location.href;
    var hasLimit = url.substring(url.lastIndexOf("&") + 1, url.lastIndexOf("=")) == "limit";
    var urlWithoutLimit = url.substring(0, url.lastIndexOf("&"));
    var hasOffset = urlWithoutLimit.substring(urlWithoutLimit.lastIndexOf("&") + 1, urlWithoutLimit.lastIndexOf("=")) == "offset";
    if (!hasLimit && !hasOffset) {
      window.location.href = constructLink(0, initialLimit());
    }
  }
  function addPage(option, off, lim, name) {
    $("#paginationUl").append("<li class='" + option + "'><a href='" + constructLink(off, lim) +
      "'>" + name + "</a></li>")
  }
  function updatePagination() {
    var nTotalPage = Math.ceil(nTotalFile / nFilePerPage);
    // less than 1 page, no need for pagination
    if (nTotalPage <= 1) return;

    var firstPage = Math.floor((currentPage - 1) / nMaxPageShown) * nMaxPageShown + 1;
    var nPageShown = Math.min(nMaxPageShown, nTotalPage - firstPage + 1);

    // first && prev
    if (currentPage > 1) {
      addPage("", 0, nFilePerPage, "First");
      addPage("", currentOffset - nFilePerPage, nFilePerPage, "Prev");
    }

    // middle
    for (var i = 0; i < nPageShown; i ++) {
      var page = firstPage + i;
      var off = (page - 1) * nFilePerPage;
      var lim = nFilePerPage;
      if (page == nTotalPage) {
        lim = nTotalFile - off;
      }
      if (page == currentPage) {
        addPage("active", off, lim, page);
      } else {
        addPage("", off, lim, page);
      }
    }

    // next && last
    if (currentPage < nTotalPage) {
      addPage("", currentOffset + currentLimit, 
        Math.min(nFilePerPage, nTotalFile - currentOffset - currentLimit), "Next");
      var off = (nTotalPage - 1) * nFilePerPage;
      addPage("", off, nTotalFile - off, "Last");
    }
  }

  // suppose offset=xxx&limit=xxx is always the last two elements of URL
  function getOffsetFromUrl() {
    var url = window.location.href;
    var urlBeforeLimit = url.substring(0, url.lastIndexOf("&"));
    var ret = parseInt(urlBeforeLimit.substring(urlBeforeLimit.lastIndexOf("=") + 1));
    if (isNaN(ret)) return 0;
    return ret;
  }
  function getLimitFromUrl() {
    var url = window.location.href;
    var urlAfterOffset = url.substring(url.lastIndexOf("&"));
    var ret = parseInt(urlAfterOffset.substring(urlAfterOffset.indexOf("=") + 1));
    if (isNaN(ret)) return 0;
    return ret;
  }
  function updateCurrentStates() {
    currentOffset = getOffsetFromUrl();
    currentLimit = getLimitFromUrl();
    currentPage = Math.floor(currentOffset / nFilePerPage) + 1;
  }
  function updateViewSettings() {
    if (Cookies.get(nFilePerPageCookie) != undefined) {
      nFilePerPage = Cookies.get(nFilePerPageCookie);
    }
    if (Cookies.get(nMaxPageShownCookie) != undefined) {
      nMaxPageShown = Cookies.get(nMaxPageShownCookie);
    }
  }
  function saveNewViewSettings() {
    var updated = false;
    var nfpg = parseInt($("#nFilePerPage").val());
    if (!isNaN(nfpg)) {
      Cookies.set(nFilePerPageCookie, nfpg);
      updated = true;
    }
    var nmps = parseInt($("#nMaxPageShown").val());
    if (!isNaN(nmps)) {
      Cookies.set(nMaxPageShownCookie, nmps);
      updated = true;
    }
    return updated;
  }
  function bindEvent() {
    $('.pagination .disabled a, .pagination .active a').on('click', function(e) {
      e.preventDefault();
    }); 

    $("#updateView").on('click', function(e) {
      if(saveNewViewSettings()) {
        window.location.href = baseUrl;
      }
    });
  }
  function updatePlaceHolderInViewSettings() {
    $("#nFilePerPage").attr("placeholder", "current value is " + nFilePerPage);
    $("#nMaxPageShown").attr("placeholder", "current value is " + nMaxPageShown);
  }
  $(document).ready(function(){
    updateViewSettings();
    redirect();
    updateCurrentStates();
    updatePagination();
    updatePlaceHolderInViewSettings();
    bindEvent();
  });
</script>
