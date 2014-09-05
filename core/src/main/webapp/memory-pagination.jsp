<script src="js/cookies.min.js" type="text/javascript"></script>
<script type="text/javascript">
  var nTotalFile = <%= request.getAttribute("inMemoryFileNum") %>;

  // view settings
  var nFilePerPage = 20;
  var nMaxPageShown = 10;

  // current states
  var currentPage = 1;
  var currentOffset = 0;
  var currentLimit = initalOffset();

  function initalOffset() {
    return Math.min(nFilePerPage, nTotalFile);
  }
  function constructLink(off, lim) {
    return './memory?offset=' + off + '&limit=' + lim;
  }
  function redirect() {
    var noOffsetAndLimit = window.location.href.toString().split(window.location.host)[1] == "/memory";
    if (noOffsetAndLimit) {
      window.location.href = constructLink(0, initalOffset());
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

    // first
    addPage("", 0, nFilePerPage, "First");

    // prev
    if (currentPage == 1) {
      addPage("disabled", 0, 0, "Prev");
    } else {
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

    // next
    if (currentPage == nTotalPage) {
      addPage("disabled", 0, 0, "Next");
    } else {
      addPage("", currentOffset + currentLimit, 
        Math.min(nFilePerPage, nTotalFile - currentOffset - currentLimit), "Next");
    }

    // last
    var off = (nTotalPage - 1) * nFilePerPage;
    addPage("", off, nTotalFile - off, "Last");

  }

  function getOffsetFromUrl() {
    var url = window.location.href;
    var ret = parseInt(url.substring(url.indexOf("=") + 1, url.indexOf("&")));
    if (isNaN(ret)) return 0;
    return ret;
  }
  function getLimitFromUrl() {
    var url = window.location.href;
    var ret = parseInt(url.substring(url.lastIndexOf("=") + 1));
    if (isNaN(ret)) return 0;
    return ret;
  }
  function updateCurrentStates() {
    currentOffset = getOffsetFromUrl();
    currentLimit = getLimitFromUrl();
    currentPage = Math.floor(currentOffset / nFilePerPage) + 1;
  }
  function updateViewSettings() {
    if (Cookies.get("nFilePerPage") != undefined) {
      nFilePerPage = Cookies.get("nFilePerPage");
    }
    if (Cookies.get("nMaxPageShown") != undefined) {
      nMaxPageShown = Cookies.get("nMaxPageShown");
    }
  }
  function saveNewViewSettings() {
    var updated = false;
    var nfpg = parseInt($("#nFilePerPage").val());
    if (!isNaN(nfpg)) {
      Cookies.set("nFilePerPage", nfpg);
      updated = true;
    }
    var nmps = parseInt($("#nMaxPageShown").val());
    if (!isNaN(nmps)) {
      Cookies.set("nMaxPageShown", nmps);
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
        window.location.href = './memory';
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
