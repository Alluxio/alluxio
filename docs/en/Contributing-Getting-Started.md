---
layout: global
title: Contributor Getting Started
nickname: Contributor Getting Started
group: Resources
---

* Table of Contents
{:toc}

Welcome to the Alluxio community! We are excited for your contributions and engagement in the
community! Here is a guide on how to get started with becoming a member in the Alluxio open source
community.

## Requirements

The main requirement is a computer with Mac OS X or Linux installed. Alluxio does not have Windows
support at the moment.

## Software and Account Preparation

There is some software and account setup required before contributing to Alluxio.

> Watch our "Alluxio new contributor" video in the [Video](#video) section below!

### Java 7 or 8

Developing Alluxio requires Java 7 or 8. If you are unsure of which version of Java you have, you
can check with this command:

```bash
$ java -version
```

If you do not have Java installed, download and install [the Java SDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

### Maven

Alluxio uses Maven to manage the build process. If you do not have Maven already installed, you can
[download Maven](https://maven.apache.org/download.cgi), and
[install Maven](https://maven.apache.org/install.html).

### Git

The Alluxio open source project uses the Git distributed version control system for its source code.
Therefore, Git needs to be installed to contribute code to Alluxio.

If you do not already have `git`, [install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

### GitHub Account

The Alluxio open source code base is hosted on GitHub in the [Alluxio repository](https://github.com/Alluxio/alluxio).

You will need a GitHub account in order to contribute to Alluxio. If you do not already have a
GitHub account, please [create a GitHub account](https://github.com/).

You will also need to know which [email addresses you have associated with your GitHub account](https://help.github.com/articles/adding-an-email-address-to-your-github-account/).

### Jira Account

The [Alluxio open source project uses JIRA](https://alluxio.atlassian.net/projects/ALLUXIO/issues)
to track features, bugs, and issues.
Please [create an Alluxio JIRA account](https://alluxio.atlassian.net/admin/users/sign-up) in
order to open tickets, track tickets, and provide fixes for tickets. If you are unable to create a
JIRA account through the portal, please send a message to jira-admin@alluxio.org in the following
format:

```
Subject: JIRA Account Request
Body:
	Email Address: example@example.com
	Username: JohnS
	Full Name: John Smith
```

## Forking the Alluxio Repository

In order to contribute code to Alluxio, you first have to fork the Alluxio repo. If you have not
already forked the repo, you can visit the [Alluxio repo](https://github.com/Alluxio/alluxio) and
press the Fork button on the top-right corner of the page. After this, you have your own fork of the
Alluxio repository.

After you forked the Alluxio repository, you should create a local clone of your fork. This will
copy the files of your fork onto your computer. You can clone your fork with this command:

```bash
$ git clone https://github.com/YOUR-USERNAME/alluxio.git
$ cd alluxio
```

This will create the clone under the `alluxio/` directory.

In order to pull changes from the open source Alluxio code base into your clone, you should create a
new remote that points to the Alluxio repository. In the directory of your newly created clone, run:

```bash
$ git remote add upstream https://github.com/Alluxio/alluxio.git
```

You can view the urls for remote repositories with the following command.

```bash
$ git remote -v
```

This will show you the urls for `origin` (your fork), and `upstream` (the Alluxio repository)

## Configuring Your Git Email

Before creating commits to Alluxio, you should verify that your Git email is setup correctly.
Please visit [the instructions for setting up your email](https://help.github.com/articles/setting-your-email-in-git/).

## Building Alluxio

Now that you have a local clone of Alluxio, you can build Alluxio!

In your local clone directory, you can build Alluxio with:

```bash
$ mvn clean install
```

This will build all of Alluxio, as well as run all the tests. This may take several minutes to
finish.

If at any point in time you would like to only recompile and not run all the checks and testing, you
can run:

```bash
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

This should take less than 1 minute.

Here are more [details for building Alluxio](Building-Alluxio-Master-Branch.html).

## Taking a New Contributor JIRA Ticket

There are multiple levels of tickets in Alluxio. The levels are:
**New Contributor**, **Beginner**, **Intermediate**, **Advanced**. New contributors to Alluxio
should do two **New Contributor** tasks before taking on more advanced tasks. **New Contributor**
tasks are quite easy to resolve and do not require too much context within the code. **Beginner**
tasks typically only need to modify a single file. **Intermediate** tasks typically need to modify
multiple files, but in the same package. **Advanced** tasks typically need to modify multiple files
from multiple packages.

All new contributors are recommended to resolve two **New Contributor** tasks before taking on
larger tasks. This is a good way to familiarize yourself with the entire process of contributing to
Alluxio.

Browse any of the open [New Contributor Alluxio Tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20NewContributor%20AND%20assignee%20in%20(EMPTY))
and find one that is unassigned. You can press the link **Assign to me** in order to assign the
ticket to yourself. You should assign a ticket to yourself before you start working on it, so others
in the community know you are working on the ticket.

Notice that all Alluxio JIRA tickets are named by **ALLUXIO-####** where **####** is a number. This
name will be important later, when you create a pull request. The title of the pull request for this
ticket will be titled like, **[ALLUXIO-XXXX] Awesome Feature**.

## Creating a Branch in your Clone

After you have taken ticket, go back to the terminal, and go to the directory of your local clone.
Now, you can start working on the fix!

In order to submit a change to Alluxio, it is best practice to do all of your changes for a single
JIRA ticket, in its own branch. Therefore, the following will show you how to create a branch.

First, make sure you are on the `master` branch in your clone. You switch to your `master` branch
with:

```bash
$ git checkout master
```

Then, you should make sure your `master` branch is in sync with the latest changes from the evolving
Alluxio code base. You pull in all the new changes in the project with the following command:

```bash
$ git pull upstream master
```

This will pull in all the changes from the Alluxio open source project, into your local `master`
branch.

Now, you can create a new branch in order to work on the **New Contributor** task you took earlier.
To create a branch name **awesome_feature**, run:

```bash
$ git checkout -b awesome_feature
```

This will create the branch, and switch to it. Now, you can modify the necessary code to address the
JIRA ticket.

## Creating Local Commits

As you are addressing the ticket, you can create local commits of your code. This can be useful for
when you have finished a well-defined portion of the change. You can stage a file for commit with:

```bash
$ git add <file to stage>
```

Once all the appropriate files are staged, you can create a local commit of those modifications
with:

```bash
$ git commit -m "<concise but descriptive commit message>"
```

If you want more details, please visit [instructions on how to create commits](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository).

## Sending a Pull Request

After you have finished all the changes to address the JIRA ticket, you are ready to submit a pull
request to the Alluxio project! Here are [detailed instructions on sending a pull request](https://help.github.com/articles/using-pull-requests/),
but the following is a common way to do it.

After you have created all necessary local commits, you can push all your commits to your repository
in GitHub. For your **awesome_feature** branch, you can push to GitHub with:

```bash
$ git push origin awesome_feature
```

This will push all of your new commits in your local branch **awesome_feature**, to the
**awesome_feature** branch in GitHub, in your fork of Alluxio.

Once you have pushed all of your changes to your fork, visit your GitHub fork of Alluxio. Usually,
this shows which of your branches have been updated recently, but if not, navigate to the branch you
want to submit the pull request for, and press the **New Pull Request** button.

In the **Open a pull request** page, the base fork should be `Alluxio/alluxio`, and the base branch
should be **master**. The head fork will be your fork, and the compare branch should be the branch
you want to submit the pull request for.

For the title of the pull request, the title should be prefixed by the JIRA ticket name. Therefore,
the title will look something like **[ALLUXIO-1234] Awesome Feature** (In the title, please replace
"Awesome Feature" with something more informative regarding your request, e.g., "Fix format in error message"
or "Improve java doc of method Foo").

In the first line of the description box, please add a link back to the JIRA ticket. The link will
look something like: `https://alluxio.atlassian.net/browse/ALLUXIO-####`

Once everything is set, click on the **Create pull request** button. Congratulations! Your first
pull request for Alluxio has been submitted!

## Reviewing the Pull Request

After the pull request has been submitted, it can be found on the
[Pull Request page of the Alluxio repository](https://github.com/Alluxio/alluxio/pulls).

After it is submitted, other developers in the community will review your pull request. Others may
add comments or questions to your pull request.

In the process, some may ask to modify parts of your pull request. In order to do that, you simply
have to make the change in the branch you were using for that pull request, create a new local
commit, push to your remote branch, and the pull request will be automatically updated. In detail:

```bash
$ git add <modified files>
$ git commit -m "<another commit message>"
$ git push origin awesome_feature
```

After all the comments and questions have been addressed in the pull request, reviewers will give
your pull request an **LGTM**. After at least 2 LGTM’s, a maintainer will merge your pull request
into the Alluxio code base.

Congratulations! You have successfully contributed to Alluxio! Thank you for joining the community!

## Video

<iframe width="560" height="315" src="https://www.youtube.com/embed/QsbM804rc6Y" frameborder="0" allowfullscreen></iframe>

## Moving to More Advanced Tasks

After you have complete two **New Contributor** tasks, you can take on some
[Beginner Alluxio tickets](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20Beginner%20AND%20assignee%20in%20(EMPTY)).

# Welcome to the Alluxio Community!
