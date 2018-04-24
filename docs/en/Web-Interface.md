---
layout: global
group: Features
title: Web Interface
priority: 6
---

* Table of Contents
{:toc}

Alluxio has a user-friendly web interface allowing users to watch and manage the system. The master
and workers all serve their own web UI. The default port for the web interface is 19999 for the
master and 30000 for the workers.

## Alluxio Master Web Interface

The Alluxio master serves a web interface to help manage the system. The default port for the
Alluxio master web interface is 19999, so the web interface can be viewed by visiting
`http://<MASTER IP>:19999`. For instance, if you started Alluxio locally, the master web interface
can be viewed by visiting [localhost:19999](http://localhost:19999).

The Alluxio master web interface contains several different pages, described below.

### Home Page

The Alluxio master home page looks something like below:

![Alluxio Master Home Page]({{site.data.img.screenshot_overview}})

The home page gives an overview of the system status. It includes the following sections:

* **Alluxio Summary** Alluxio system level information

* **Cluster Usage Summary** Alluxio storage information as well as under storage information. Alluxio storage utilization can be near 100%, but under storage utilization should not approach 100%.

* **Storage Usage Summary** Alluxio tiered storage information which gives a break down of amount of space used per tier across the Alluxio cluster.

### Configuration Page

To check the current configuration information, click "Configuration" in the
navigation bar on the top of the screen.

![configurations]({{site.data.img.screenshot_systemConfiguration}})

The configuration page has two sections:

* **Alluxio Configuration** A map of all the Alluxio configuration properties and their set values.

* **White List** Contains all the Alluxio path prefixes eligible to be stored in Alluxio. A request may still be made to a file not prefixed by a path in the white list. Only whitelisted files will be stored in Alluxio.

### Browse File System

You can browse the Alluxio file system through the UI. When selecting the "Browse" tab
in the navigation bar, you will see something like this:

![browse]({{site.data.img.screenshot_browseFileSystem}})

Files in the current folder are listed, with the file name, file size, size for each block, the
percentage of in-Alluxio data, creation time, and the modification time. To view the content of a
file, click on that file.

![viewFile]({{site.data.img.screenshot_viewFile}})

### Browse In-Alluxio Files Page

To browse all in-Alluxio files, click on the "In-Alluxio Files" tab in the navigation bar.

![inMemFiles]({{site.data.img.screenshot_inMemoryFiles}})

Files currently in Alluxio are listed, with the file name, file size, size for each block,
whether the file is pinned or not, the file creation time, and the file modification time.

### Workers Page

The master also shows all known Alluxio workers in the system and shows them in the "Workers" tab.

![workers]({{site.data.img.screenshot_workers}})

The workers page gives an overview of all Alluxio worker nodes divided into two sections:

* **Live Nodes** A list of all the workers currently serving Alluxio requests. Clicking on the worker name will redirect to the worker's web UI.

* **Dead Nodes** A list of all workers proclaimed as dead by the master, usually due to a long timeout waiting for the worker heartbeat. Possible causes include system restart or network failures.

### Master Metrics

To Access master metrics section, click on the “Metrics” tab in the navigation bar.

![masterMetrics]({{site.data.img.screenshot_masterMetrics}})

This section shows all master metrics. It includes the following sections:

* **Master Gauges** Overall measures of the master.

* **Logical Operation** Number of operations performed.

* **RPC Invocation** Number of RPC invocations per operation.

## Alluxio Workers Web Interface

Each Alluxio worker also serves a web interface to show worker information. The default port for the
worker web interface is 30000 so the web interface can be viewed by visiting
`http://<WORKER IP>:30000`. For instance, if you started Alluxio locally, the worker web interface
can  be viewed by visiting [localhost:30000](http://localhost:30000).

### Home Page

The home page for the Alluxio worker web interface is similar to the home page for the Alluxio
master, but shows information specific to a single worker. Therefore, it has similar sections:
**Worker Summary**, **Storage Usage Summary**, **Tiered Storage Details**.

### BlockInfo Page

In the "BlockInfo" page, you can see the files on the worker, and other information such as the
file size and which tiers the files is stored on. Also, if you click on a file, you can view all
the blocks of that file.

### Worker Metrics 

To Access worker metrics section, click on the “Metrics” tab in the navigation bar.

![workerMetrics]({{site.data.img.screenshot_workerMetrics}})

This section shows all worker metrics. It includes the following sections:

* **Worker Gauges** Overall measures of the worker.

* **Logical Operation** Number of operations performed.
