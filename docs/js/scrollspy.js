// JavaScript implementation of ScrollSpy
// finds toc and automatically sorts by main list items as parents and nested list items as children

var toc = document.getElementById("toc");
if (toc !== null) { // checks to see if table of contents exists in page
    toc.classList.add("on");
    var topPadding = 12; // amount of space above header to start noticing a section
    var navBar = document.getElementById("topbar").offsetHeight;
    var tocEl = toc.nextElementSibling.getElementsByTagName("li"); // table of contents list
    var sections = toc.nextElementSibling.getElementsByTagName("a"); // table of contents links
    sections = Array.prototype.slice.call(sections);
    var sectionsLen = sections.length;
    var parentIndex;
    var parentTag = sections[0].getAttribute("href") == "#" ? "H1" : document.getElementById(sections[0].getAttribute("href").substring(1)).nodeName;
    for (var i = 0; i < sectionsLen; i++) {
        // goes through links and creates a 2d array with:
        // [element, offset from top, group]
        var element = sections[i].getAttribute("href") == "#" ? document.getElementById("content").firstElementChild : document.getElementById(sections[i].getAttribute("href").substring(1));
        if (element.nodeName == parentTag) {
            parentIndex = i;
        }
        sections[i] = [element, element.offsetTop - navBar - topPadding, parentIndex];
    }
    var active = 0;
    tocEl[0].classList.toggle("active");
    var sublist;
    var sublistPos = 0;
    var sublistChildren = 0;
    var locked = false;
    var runOnScroll =  function(e) {
        if (locked) return; // if still scrolling, do not run
        locked = true;
        var pos = 0;

        // get current active element
        for (var i = 0; i < sectionsLen; i++) {
            if (window.scrollY > sections[i][1]) {
                pos = i;
            } else {
                break;
            }
        }

        if (active != pos) { // change in active element
            tocEl[active].classList.remove('active');
            active = pos;
            tocEl[active].classList.add('active');
            if (sublist !== null) { // currently in a sublist
                sublistPos = active - sublist;
                if (sublistPos > sublistChildren || sublistPos < 1) {
                    // the position in the sublist is out of range so the sublist has been exited
                    tocEl[sublist].classList.toggle('list');
                    sublist = null;
                    sublistPos = sublistChildren = 0;
                }
            }

            if (tocEl[active].children[1] != null && tocEl[active].children[1].nodeName == "UL") {
                // this list item has a nested list so a sublist has been entered
                sublist = active;
                sublistChildren = tocEl[active].children[1].childElementCount;
                tocEl[sublist].classList.toggle('list');
            } else if (sections[active][0].nodeName != parentTag) {
                // the current item is not of the header type of the parent items,
                // so it is a sublist item and a sublist has been entered
                sublist = sections[active][2];
                tocEl[sublist].classList.add('list');
                sublistChildren = tocEl[sublist].children[1].childElementCount;
            }
        }
        locked = false;
    }

    window.addEventListener("scroll", runOnScroll);
}
