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

An email must be associated with your GitHub account in order to make
contributions. You can check this in [your profile email settings](https://github.com/settings/emails).

### Configuring Your Git Email

Before creating commits to Alluxio, you should verify that your committer email in Git is set up correctly.
Please visit
[the instructions for setting up your email](https://help.github.com/articles/setting-your-email-in-git/).

## Forking the Alluxio Repository

In order to contribute code to Alluxio, fork the Alluxio repository (repo).
If you have not already forked the repo, you can visit the
[Alluxio repository](https://github.com/Alluxio/alluxio) and press the Fork button on the
top-right corner of the page. After this, you have your own fork of the
Alluxio repository under your GitHub account.

Create a local clone of your fork to copy the files of your fork onto your computer.
Clone your fork with this command:

```console
$ git clone https://github.com/YOUR-USERNAME/alluxio.git
$ cd alluxio
```

This will create the clone under the `alluxio/` directory.
The remaining commands assume the current working directory to be at this directory,
the cloned repository root.

Set a new remote repository that points to the Alluxio repository
to pull changes from the open source Alluxio codebase into your clone.
To add a new remote repository, run:

```console
$ git remote add upstream https://github.com/Alluxio/alluxio.git
```

This will create a remote repository called `upstream` pointing to the Alluxio repository.
View the urls for remote repositories with the following command.

```console
$ git remote -v
```

This will show you the urls for the remote repositories,
including `origin` (your fork), and `upstream` (the Alluxio repository).

## Building Alluxio

Build Alluxio by running:

```console
$ mvn clean install
```

This will build Alluxio, resulting in a series of compiled jars,
as well as run all checks and tests.
Depending on your hardware, this may take anywhere form several minutes to half an hour to finish.

To only recompile and skip running all the checks and tests, run:

```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip \
  -Dcheckstyle.skip -Dlicense.skip
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

All new contributors are recommended to resolve one and only one **New Contributor** task before taking on
larger tasks. This is a good way to familiarize yourself with the entire process of contributing to
the Alluxio project.

Browse any of the open [New Contributor Alluxio Tasks](https://github.com/Alluxio/new-contributor-tasks/issues)
and find one that is unassigned. 
In order to assign an issue to yourself, leave a comment in the issue like `/assign @yourUserName`
to indicate that you are working on the issue.
You should always assign a ticket to yourself this way before you start working on it, so others
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

Keep all changes for a single issue in its own branch when submitting a change to Alluxio.

Ensure you are on the `master` branch in your clone. Switch to the `master` branch with:

```console
$ git checkout master
```

The `master` branch needs to be in sync with the latest changes from the evolving
Alluxio codebase. Pull new changes from the project with:

```console
$ git pull upstream master
```

This pulls in the changes from the `master` branch of the `upstream` repository into your local `master`
branch, where the `upstream` repository was previously set to the Alluxio open source project.

Create a new branch to work on the **New Contributor** task you took earlier.
To create a branch name **awesome_feature**, run:

```console
$ git checkout -b awesome_feature
```

This will create the branch and switch to it. Now, you can modify the necessary code to address the
issue.

### Creating Local Commits

As you are addressing the ticket, you can create local commits of your code. This can be useful for
when you have finished a well-defined portion of the change. You can stage a file for a commit with:

```console
$ git add <file to stage>
```

Once the appropriate files are staged, create a local commit of these modifications with:

```console
$ git commit -m "<concise but descriptive commit message>"
```

Please read the [Alluxio coding conventions]({{ '/en/contributor/Code-Conventions.html' | relativize_url }})
for more details and tips on how to update the Alluxio source code.

For more details for creating commits, please visit [instructions on how to create
commits](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository).

### Sending a Pull Request

After you have finished all the changes to address the issue, you are ready to submit a pull
request to the Alluxio project! Here are [detailed instructions on sending a pull
request](https://help.github.com/articles/using-pull-requests/),
but the following is a common way to do it.

Push the branch with your commits to your forked repository in GitHub.
For your **awesome_feature** branch, push to GitHub with:

```console
$ git push origin awesome_feature
```

Visit your GitHub fork of Alluxio. Usually, this shows which of your branches have been updated recently,
but if not, navigate to the branch you want to submit the pull request for (**awesome_feature** in this example),
and press the **Pull request** button next to the **Compare** button.

In the **Comparing changes** page, the base repository should be `Alluxio/alluxio` and the base branch
should be **master**. The head repository should be your fork and the compare branch should be the branch
you want to submit the pull request for (**awesome_feature** in this example).
Click on the **Create pull request** button.
The title of the page will change to **Open a pull request** and you should see the boxes that let you input the title
and the description of your pull request.

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
add comments or questions to your pull request. Tests and checks will also be run against the
new changes to validate your changes are safe to merge.

During the code review process, please reply to all comments left by reviewers to track which
comments have been addressed and how each comment has been addressed.

Additional code changes may be necessary to address comments from reviewers or fix broken tests and checks.
After making the required changes locally, create a new commit and push to your remote branch.
GitHub will detect new changes to the source branch and update the corresponding pull request automatically.
An example workflow to update your remote branch:

```console
$ git add <modified files>
$ git commit -m "<another commit message>"
$ git push origin awesome_feature
```

After all the comments and questions have been addressed in the pull request, reviewers will give
your pull request an **LGTM** and approve the pull request. After at least one approval,
a maintainer will merge your pull request into the Alluxio codebase.

Congratulations! You have successfully contributed to Alluxio! Thank you for joining the community!

## Video

<iframe width="560" height="315" src="https://www.youtube.com/embed/QsbM804rc6Y" frameborder="0" allowfullscreen></iframe>

## Submit Your Feature

If you have a brilliant idea about a new Alluxio feature, we highly encourage you to implement it and 
contribute to the Alluxio repository.

1. Create a Github issue in [Alluxio repository](https://github.com/Alluxio/alluxio/issues) regarding your feature.
1. Attach a feature design document following the 
[template]({{ '/resources/templates/Design-Document-Template.md' | relativize_url }})
in your Github issue. The design document should follow the template and provide information regarding each section.
We recommend using a public Google doc for more efficient collaboration and discussions.
If a Google doc is not an option for you, you can choose to attach either a Markdown file or a PDF file for 
the design document.
1. Tag your issue `type-feature`. Alluxio members periodically check all open issues and will allocate reviewers 
to the design document.
Community users do not have the access to assign issues in Alluxio repository. 
So if you would like to initiate the review process, you can create a placeholder pull request, 
link to the issue, attach the feature design document, and request reviews from Alluxio members there.
1. The reviewers (assignees on the corresponding Github issue or the reviewers of the pull request) will review
the feature design document and provide feedback or request for iterations.
Please feel free to raise questions and seek help from the reviewers and community members, 
if you are uncertain about specific design decisions. 
Please also list options for the feature and corresponding pros and cons.
1. After a few iterations on the design document, reviewers will give an **LGTM** to the design document.
And this feature can move on to the implementation phase.
1. Fork Alluxio repository, implement your feature and create a pull request, 
as you have mastered in the previous section. Please also link to the issue and design document in your pull request.
In the pull request, you should also add documentation on your feature to 
[Alluxio documentations](https://docs.alluxio.io/).
1. After your pull request is reviewed and merged, you have contributed your feature to Alluxio. Cheers!

## Next Steps

There are a few things that new contributors can do to familiarize themselves with Alluxio:

1.  [Run Alluxio Locally]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
1.  [Run Alluxio on a Cluster]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }})
1.  Read [Configuration Settings]({{ '/en/operation/Configuration.html' | relativize_url }}) and
[Command Line Interface]({{ '/en/operation/User-CLI.html' | relativize_url }})
1.  Read a [Code Example](https://github.com/Alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/MultiMount.java)
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
