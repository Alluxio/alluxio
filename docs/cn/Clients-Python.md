---
layout: global
title: Python Client
nickname: Python Client
group: Clients
priority: 5
---

AlluxioÓÐÒ»¸öPython¿Í»§¶Ë£¬Õâ¸ö¿Í»§¶Ë¿ÉÒÔÍ¨¹ýËüµÄREST APIÀ´ºÍAlluxio½»Á÷¡£Ëü·¢²¼ÁËÒ»¸öºÍ±¾µØµÄJava APIÀàËÆµÄAPI¡£Í¨¹ý¿´ÕâÆªdocÀ´ÁË½âÓÐ¹ØËùÓÐ¿ÉÓÃ·½·¨µÄÏêÏ¸ÎÄµµ¡£Í¨¹ý¿´Àý×ÓÀ´ÁË½âÈçºÎÔÚAlluxioÉÏÖ´ÐÐ»ù±¾µÄÎÄ¼þÏµÍ³²Ù×÷¡£

# Alluxio´úÀíÒÀÀµ
Õâ¸öPython¿Í»§¶ËÍ¨¹ýÓÉAlluxio´úÀíÌá¹©µÄREST APIÀ´ºÍAlluxioÏà»¥½»Á÷¡£
Õâ¸ö´úÀí·þÎñÆ÷ÊÇÒ»¸ö¶ÀÁ¢µÄ·þÎñÆ÷£¬¿ÉÒÔÍ¨¹ý${ALLUXIO_HOME}/bin/alluxio-start.sh proxyÀ´¿ªÆôËüºÍÍ¨¹ýÃüÁî${ALLUXIO_HOME}/bin/alluxio-stop.sh proxyÀ´¹Ø±ÕËü¡£Ä¬ÈÏÇé¿öÏÂ£¬¿ÉÒÔÍ¨¹ý¶Ë¿Ú39999À´Ê¹ÓÃREST API¡£
Ê¹ÓÃHTTP´úÀí·þÎñÆ÷ÓÐÐÔÄÜÉÏµÄÓ°Ïì¡£ÌØ±ðµÄÊÇ£¬Ê¹ÓÃÕâ¸ö´úÀí·þÎñÆ÷ÐèÒªÒ»¸ö¶îÍâµÄÌø¡£ÎªÁË´ïµ½×î¼ÑÐÔÄÜ£¬ÔËÐÐÕâ¸ö´úÀí·þÎñÆ÷µÄÊ±ºòÍÆ¼öÔÚÃ¿¸ö¼ÆËã½ÚµãÉÏ·ÖÅäÒ»¸öAlluxio worker¡£

# °²×°python¿Í»§¶Ë¿â
ÃüÁî£º$ pip install alluxio

# Ê¹ÓÃÊ¾Àý
ÏÂÃæµÄ³ÌÐòÊ¾Àý°üÀ¨ÁËÈçºÎÔÚAlluxio´´½¨Ä¿Â¼¡¢ÏÂÔØ¡¢ÉÏ´«¡¢¼ì²éÎÄ¼þÊÇ·ñ´æÔÚÒÔ¼°ÎÄ¼þÁÐ±í×´Ì¬¡£
=======
å¸ƒå±€ï¼šå…¨å±€
æ ‡é¢˜ï¼šPythonå®¢æˆ·ç«¯
æ˜µç§°ï¼šPythonå®¢æˆ·ç«¯
ç¾¤ç»„ï¼šå®¢æˆ·
ä¼˜å…ˆçº§ï¼š5
Alluxioæœ‰ä¸€ä¸ªPythonå®¢æˆ·ç«¯ï¼Œè¿™ä¸ªå®¢æˆ·ç«¯å¯ä»¥é€šè¿‡å®ƒçš„REST APIæ¥å’ŒAlluxioäº¤æµã€‚å®ƒå‘å¸ƒäº†ä¸€ä¸ªå’Œæœ¬åœ°çš„Java APIç±»ä¼¼çš„APIã€‚é€šè¿‡çœ‹è¿™ç¯‡docæ¥äº†è§£æœ‰å…³æ‰€æœ‰å¯ç”¨æ–¹æ³•çš„è¯¦ç»†æ–‡æ¡£ã€‚é€šè¿‡çœ‹ä¾‹å­æ¥äº†è§£å¦‚ä½•åœ¨Alluxioä¸Šæ‰§è¡ŒåŸºæœ¬çš„æ–‡ä»¶ç³»ç»Ÿæ“ä½œã€‚

**alluxioä»£ç†ä¾èµ–**
è¿™ä¸ªPythonå®¢æˆ·ç«¯é€šè¿‡ç”±Alluxioä»£ç†æä¾›çš„REST APIæ¥å’ŒAlluxioç›¸äº’äº¤æµã€‚
è¿™ä¸ªä»£ç†æœåŠ¡å™¨æ˜¯ä¸€ä¸ªç‹¬ç«‹çš„æœåŠ¡å™¨ï¼Œå¯ä»¥é€šè¿‡${ALLUXIO_HOME}/bin/alluxio-start.sh proxyæ¥å¼€å¯å®ƒå’Œé€šè¿‡å‘½ä»¤${ALLUXIO_HOME}/bin/alluxio-stop.sh proxyæ¥å…³é—­å®ƒã€‚é»˜è®¤æƒ…å†µä¸‹ï¼Œå¯ä»¥é€šè¿‡ç«¯å£39999æ¥ä½¿ç”¨REST APIã€‚
ä½¿ç”¨HTTPä»£ç†æœåŠ¡å™¨æœ‰æ€§èƒ½ä¸Šçš„å½±å“ã€‚ç‰¹åˆ«çš„æ˜¯ï¼Œä½¿ç”¨è¿™ä¸ªä»£ç†æœåŠ¡å™¨éœ€è¦ä¸€ä¸ªé¢å¤–çš„è·³ã€‚ä¸ºäº†è¾¾åˆ°æœ€ä½³æ€§èƒ½ï¼Œè¿è¡Œè¿™ä¸ªä»£ç†æœåŠ¡å™¨çš„æ—¶å€™æŽ¨èåœ¨æ¯ä¸ªè®¡ç®—èŠ‚ç‚¹ä¸Šåˆ†é…ä¸€ä¸ªAlluxio workerã€‚

**å®‰è£…pythonå®¢æˆ·ç«¯åº“**
å‘½ä»¤ï¼š$ pip install alluxio

**ä½¿ç”¨ç¤ºä¾‹**
ä¸‹é¢çš„ç¨‹åºç¤ºä¾‹åŒ…æ‹¬äº†å¦‚ä½•åœ¨Alluxioåˆ›å»ºç›®å½•ã€ä¸‹è½½ã€ä¸Šä¼ ã€æ£€æŸ¥æ–‡ä»¶æ˜¯å¦å­˜åœ¨ä»¥åŠæ–‡ä»¶åˆ—è¡¨çŠ¶æ€ã€‚
>>>>>>> c08ecfb6e52eb5aefa904c934241a2aa9dcd5178
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys

import alluxio
from alluxio import option


def colorize(code):
    def _(text, bold=False):
        c = code
        if bold:
            c = '1;%s' % c
        return '\033[%sm%s\033[0m' % (c, text)
    return _

green = colorize('32')


def info(s):
    print green(s)


def pretty_json(obj):
    return json.dumps(obj, indent=2)


def main():
    py_test_root_dir = '/py-test-dir'
    py_test_nested_dir = '/py-test-dir/nested'
    py_test = py_test_nested_dir + '/py-test'
    py_test_renamed = py_test_root_dir + '/py-test-renamed'
    
    client = alluxio.Client('localhost', 39999)
    
    info("creating directory %s" % py_test_nested_dir)
    opt = option.CreateDirectory(recursive=True)
    client.create_directory(py_test_nested_dir, opt)
    info("done")
    
    info("writing to %s" % py_test)
    with client.open(py_test, 'w') as f:
        f.write('Alluxio works with Python!\n')
        with open(sys.argv[0]) as this_file:
            f.write(this_file)
    info("done")
    
    info("getting status of %s" % py_test)
    stat = client.get_status(py_test)
    print pretty_json(stat.json())
    info("done")
    
    info("renaming %s to %s" % (py_test, py_test_renamed))
    client.rename(py_test, py_test_renamed)
    info("done")
    
    info("getting status of %s" % py_test_renamed)
    stat = client.get_status(py_test_renamed)
    print pretty_json(stat.json())
    info("done")
    
    info("reading %s" % py_test_renamed)
    with client.open(py_test_renamed, 'r') as f:
        print f.read()
    info("done")
    
    info("listing status of paths under /")
    root_stats = client.list_status('/')
    for stat in root_stats:
        print pretty_json(stat.json())
    info("done")
    
    info("deleting %s" % py_test_root_dir)
    opt = option.Delete(recursive=True)
    client.delete(py_test_root_dir, opt)
    info("done")
    
    info("asserting that %s is deleted" % py_test_root_dir)
    assert not client.exists(py_test_root_dir)
    info("done")


if __name__ == '__main__':
    main()
