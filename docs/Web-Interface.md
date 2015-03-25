---
layout: global
title: Web Interface
---

Tachyon has a user-friendly web interface allowing users to watch and manage the system. Below we
describe its details.

The **home page** gives an overview of the system's status.

![home](./img/screenshot_overview.png)

To check the list of all workers, click "Workers" button. In **workers page**, all nodes are listed,
also the last heartbeat, node state, workers capacity, space used in bytes, as well as space
usage percentage are presented.

![workers](./img/screenshot_workers.png)

To check current system configuration information, click "System Configuration" button.

![configurations](./img/screenshot_systemConfiguration.png)

To browse the list of files, click "Browse File System" button. In **browsing page**, files in the
current folder are listed, with the file name, file size, size for each block, the percentage of
in-memory data, and the creation time. To view the content of a file in detail, click on that file.

![browse](./img/screenshot_browseFileSystem.png)

To browse all in-memory files, click "In Memory Files" button. In **in memory files page**, files
currently in memory are listed, with the file name, file size, size for each block, whether the
file is pinned or not, the file creation time, and the file modification time.

![inMemFiles](./img/screenshot_inMemoryFiles.png)