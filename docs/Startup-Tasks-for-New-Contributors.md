---
layout: global
title: Startup Tasks for New Contributors
---

A list of tasks that everyone should do before contributing to Tachyon.

1.  [Running Tachyon Locally](Running-Tachyon-Locally.html)

2.  [Running Tachyon on a Cluster](Running-Tachyon-on-a-Cluster.html)
    (Optional)

3.  Read
    [Configuration-Settings](Configuration-Settings.html)
    (Optional) and play
    [Command-Line Interface](Command-Line-Interface.html)
    (Optional)

4.  Read and understand [an example](https://github.com/amplab/tachyon/blob/master/src/main/java/tachyon/examples/BasicOperations.java).

5.  [Building Tachyon Master Branch](Building-Tachyon-Master-Branch.html).

6.  Fork the repository, write/add unit tests/java doc for one or two files in the following list,
and then submit a pull request. You are also welcome to address issues in our
[JIRA](https://spark-project.atlassian.net/browse/TACHYON)

* * * * *

    src/main/java/tachyon/Users.java

    src/main/java/tachyon/master/MasterWorkerInfo.java

    src/main/java/tachyon/worker/Worker.java

    src/main/java/tachyon/worker/WorkerClient.java

    src/main/java/tachyon/worker/DataServerMessage.java

#### After the pull request is reviewed and merged, you become a Tachyon contributor!

### Submitting Code

-   Break your work into small, single-purpose patches if possible. It’s
    much harder to merge in a large change with a lot of disjoint
    features.
-   Submit the patch as a GitHub pull request. For a tutorial, see the
    GitHub guides on [forking a repo](https://help.github.com/articles/fork-a-repo) and
    [sending a pull request](https://help.github.com/articles/using-pull-requests).
-   Follow the style of the existing codebase. Specifically, we use
    [Sun's conventions](http://www.oracle.com/technetwork/java/codeconv-138413.html),
    with the following changes:
    -   Indent two spaces per level, not four.
    -   Maximum line length of 100 characters.
    -   `i ++` instead of `i++`
    -   `i + j` instead of `i+j`

-   Make sure that your code passes the unit tests.
