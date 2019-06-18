---
layout: global
title: Contribution Guide
nickname: Contribution Guide
group: Contributor Resources
priority: 1
---

We warmly welcome you to the Alluxio community. We are excited for your contributions and
engagement with our project! This guide aims to give you step by step instructions on how
to get started becoming a contributor to the Alluxio open source project.

* Table of Contents
{:toc}

## Prerequisites

The main requirement is a computer with MacOS or Linux-based operating system installed. Alluxio
does not have Windows support at this time.

If you haven't already, we recommend first cloning and compiling the Alluxio source code with our
[Building Alluxio from Source Tutorial]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).

### Software Requirements

- Required Software:
  - Java 8
  - Maven 3.3.9+
  - Git

### Account Preparation

#### Github Account

A GitHub account is required in order to contribute to the Alluxio repository.

You will need to know an email address that is associated with your GitHub account in order to make
contributions. You can check this in [your profile email settings](https://github.com/settings/emails).

### Configuring Your Git Email

Before creating commits to Alluxio, you should verify that your Git email is setup correctly.
Please visit
[the instructions for setting up your email](https://help.github.com/articles/setting-your-email-in-git/).

## Forking the Alluxio Repository

In order to contribute code to Alluxio, you first have to fork the Alluxio repository (repo).
If you have not already forked the repo, you can visit the
[Alluxio repository](https://github.com/Alluxio/alluxio) and press the Fork button on the
top-right corner of the page. After this, you have your own fork of the
Alluxio repository.

After you fork the Alluxio repository, you should create a local clone of your fork. This will
copy the files of your fork onto your computer. You can clone your fork with this command:

```bash
git clone https://github.com/YOUR-USERNAME/alluxio.git
cd alluxio
```

This will create the clone under the `alluxio/` directory.

In order to pull changes from the open source Alluxio code base into your clone, you should create a
new remote repository that points to the Alluxio repository.
In order to add a new remote repository, in the directory of your newly created clone, run:

```bash
git remote add upstream https://github.com/Alluxio/alluxio.git
```

This will create a remote repository called `upstream` pointing to the Alluxio repository.
You can view the urls for remote repositories with the following command.

```bash
git remote -v
```

This will show you the urls for the remote repositories,
including `origin` (your fork), and `upstream` (the Alluxio repository).

## Building Alluxio

Now that you have a local clone of Alluxio, you can build Alluxio!

In your local clone directory, you can build Alluxio with:

```bash
mvn clean install
```

This will build all of Alluxio, as well as run all the tests. Depending on your hardware this
may take anywhere form several minutes to half an hour to finish.

If at any point in time you would like to only recompile and not run all the checks and testing, you
can run:

```bash
mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

This should take less than 1 minute.

Here are more
[details for building Alluxio]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }}).

## Taking a New Contributor Task

There are multiple levels of tickets in Alluxio. The levels are:
**New Contributor**, **Beginner**, **Intermediate**, **Advanced**. New contributors to Alluxio
should do two **New Contributor** tasks before taking on more advanced tasks. **New Contributor**
tasks are quite easy to resolve and do not require too much context within the code. **Beginner**
tasks typically only need to modify a single file. **Intermediate** tasks typically need to modify
multiple files, but in the same package. **Advanced** tasks typically need to modify multiple files
from multiple packages.

All new contributors are recommended to resolve two **New Contributor** tasks before taking on
larger tasks. This is a good way to familiarize yourself with the entire process of contributing to
the Alluxio project.

Browse any of the open [New Contributor Alluxio Tasks](https://github.com/Alluxio/new-contributor-tasks/issues)
and find one that is unassigned. 
In order to assign an issue to yourself, leave a comment in the issue like `/assign @yourUserName`
to indicate that you are working on the issue.
You should assign a ticket to yourself before you start working on it, so others
in the community know you are working on the ticket.

Notice that all New Contributor issues on Github are assigned with a number. The number can be
found after the issue title, like `#123`.
When you create a pull request to address the issue, you should add a link/pointer back to the
issue itself. In order to do that you have to add certain text in the pull request description.
For example, if your issue number is `#123`, you should include one of the following in your
pull request description.
  * `Fixes Alluxio/new-contributor-tasks#123`
  * `Fixed Alluxio/new-contributor-tasks#123`
  * `Fix Alluxio/new-contributor-tasks#123`
  * `Closes Alluxio/new-contributor-tasks#123`
  * `Closed Alluxio/new-contributor-tasks#123`
  * `Close Alluxio/new-contributor-tasks#123`

### Creating a Branch in your Clone

After you have taken an issue, go back to the terminal, and go to the directory of your local clone.
Now, you can start working on the fix!

In order to submit a change to Alluxio, it is best practice to do all of your changes for a single
issue, in its own branch. Therefore, the following will show you how to create a branch.

First, make sure you are on the `master` branch in your clone. You switch to your `master` branch
with:

```bash
git checkout master
```

Then, you should make sure your `master` branch is in sync with the latest changes from the evolving
Alluxio code base. You pull in all the new changes in the project with the following command:

```bash
git pull upstream master
```

This will pull in all the changes from the `master` branch of the `upstream` repository into your local `master`
branch. In this example, the `upstream` repository is the Alluxio open source project.

Now, you can create a new branch in order to work on the **New Contributor** task you took earlier.
To create a branch name **awesome_feature**, run:

```bash
git checkout -b awesome_feature
```

This will create the branch, and switch to it. Now, you can modify the necessary code to address the
issue.

### Creating Local Commits

As you are addressing the ticket, you can create local commits of your code. This can be useful for
when you have finished a well-defined portion of the change. You can stage a file for commit with:

```bash
git add <file to stage>
```

Once all the appropriate files are staged, you can create a local commit of those modifications
with:

```bash
git commit -m "<concise but descriptive commit message>"
```

Please read the [Alluxio coding conventions]({{ '/en/contributor/Code-Conventions.html' | relativize_url }})
for more details and tips on how to update the Alluxio source code.

If you want more details for creating commits, please visit [instructions on how to create
commits](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository).

### Sending a Pull Request

After you have finished all the changes to address the issue, you are ready to submit a pull
request to the Alluxio project! Here are [detailed instructions on sending a pull
request](https://help.github.com/articles/using-pull-requests/),
but the following is a common way to do it.

After you have created all necessary local commits, you can push all your commits to your repository
in GitHub. For your **awesome_feature** branch, you can push to GitHub with:

```bash
git push origin awesome_feature
```

This will push all of your new commits in your local branch **awesome_feature**, to the
**awesome_feature** branch in GitHub, in your fork of Alluxio.

Once you have pushed all of your changes to your fork, visit your GitHub fork of Alluxio. Usually,
this shows which of your branches have been updated recently, but if not, navigate to the branch you
want to submit the pull request for (**awesome_feature** in this example),
and press the **New Pull Request** button.

In the **Open a pull request** page, the base fork should be `Alluxio/alluxio`, and the base branch
should be **master**. The head fork will be your fork, and the compare branch should be the branch
you want to submit the pull request for (**awesome_feature** in this example).

#### Pull Request Title

It is important to use an effective title for the pull request. Here are some tips and conventions
for a great PR title. These tips are adapted from
[existing rules for great commit messages](https://chris.beams.io/posts/git-commit/#seven-rules).

* Title should be not too long (< ~50 characters) and not too short (descriptive enough)
* Title should start with an imperative verb
  * Examples: `Fix Alluxio UI bugs`, `Refactor Inode caching logic`
  * Incorrect: `~~Fixed Alluxio UI bugs~~`, `~~Inode caching refactor~~`
  * [List of allowable words to start the title](https://github.com/Alluxio/alluxio/blob/master/docs/resources/pr/pr_title_words.md)
* The first word of the title should be capitalized
* Title should not end with a period

There are a few exceptions to these rules. Prefixes can be added to the beginning of the title.
Prefixes are in all caps and is separated from the rest of the title with a space. Here are the
possible prefixes.
* **[DOCFIX]**: This is for PRs which updates documentation
  * Examples: `[DOCFIX] Update the Getting Started guide`, `[DOCFIX] Add GCS documentation`
* **[SMALLFIX]**: This is for PRs for minor fixes which do not change any logic, like typos
  * Examples: `[SMALLFIX] Fix typo in AlluxioProcess`, `[SMALLFIX] Improve comment style in GlusterFSUnderFileSystem`

#### Pull Request Description

It is also important to write a good PR description. Here are some tips and conventions for a great
PR description, adapted from
[existing rules for great commit messages](https://chris.beams.io/posts/git-commit/#seven-rules).

* Description should explain what this PR is changing and why this change is being made
* Description should include any positive and negative implications of the change
* Paragraphs in the description should be separated by a blank line
* If this pull request is addressing a Github issue, please add a link back to the issue on the
**last** line of the description box.
  * If this PR solves Github Issue `#1234` include `Fixes #1234`, `Fixed #1234`, `Fix #1234`, `Closes #1234`,
`Closed #1234`, or `Close #1234` at the bottom of the pull request description.
  * If the issue is from new contributor tasks, prefix the issue number `#1234` with repository name
`Alluxio/new-contributor-tasks`, like `Fixes Alluxio/new-contributor-tasks#1234`.

Once everything is set, click on the **Create pull request** button. Congratulations! Your first
pull request for Alluxio has been submitted!

### Reviewing the Pull Request

After the pull request has been submitted, it can be found on the
[Pull Request page of the Alluxio repository](https://github.com/Alluxio/alluxio/pulls).

After it is submitted, other developers in the community will review your pull request. Others may
add comments or questions to your pull request.

During the code review, please reply to all comments left by reviewers, so reviewers know which
comments have been addressed, and how each comment has been addressed.

In the process, some may ask to modify parts of your pull request. In order to do that, you simply
have to make the change in the branch you were using for that pull request, create a new local
commit, push to your remote branch, and the pull request will be automatically updated. In detail:

```bash
git add <modified files>
git commit -m "<another commit message>"
git push origin awesome_feature
```

After all the comments and questions have been addressed in the pull request, reviewers will give
your pull request an **LGTM** and approve the pull request. After at least one approval,
a maintainer will merge your pull request into the Alluxio code base.

Congratulations! You have successfully contributed to Alluxio! Thank you for joining the community!

## Video

<iframe width="560" height="315" src="https://www.youtube.com/embed/QsbM804rc6Y" frameborder="0" allowfullscreen></iframe>

## Next Steps

There are a few things that new contributors can do to familiarize themselves with Alluxio:

1.  [Run Alluxio Locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
1.  [Run Alluxio on a Cluster]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }})
1.  Read [Configuration Settings]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}) and
[Command Line Interface]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }})
1.  Read a [Code Example](https://github.com/alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)
1.  [Build Alluxio From Source]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }})
1.  Fork the repository, add unit tests or javadoc for one or two files, and submit a pull request.
You are also welcome to address issues in our [Github Issues](https://github.com/Alluxio/alluxio/issues).
Here is a list of unassigned
[New Contributor Tasks](https://github.com/Alluxio/new-contributor-tasks/issues).
Please limit 2 tasks per New Contributor.
Afterwards, try some Beginner/Intermediate tasks, or ask in the
[User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).

## Welcome to the Alluxio Community!
