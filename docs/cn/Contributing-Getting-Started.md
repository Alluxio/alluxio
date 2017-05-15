---
layout: global
title: 开发者上手指南
nickname: 开发者上手指南
group: Resources
---

* 内容列表
{:toc}

欢迎来到Alluxio社区，我们热烈欢迎你参与到我们的社区和对社区进行贡献。本页面提供了如何参与到Alluxio开源项目社区并成为其中一员的指导。

## 系统需要

最基本的需要是一台安装了Mac OS X或者Linux的电脑，目前版本的Alluxio尚未对Windows系统提供支持。

## 软件和相关账户准备

在向Alluxio贡献源码之前，还需要准备一些软件以及账户。

> 在下面的[视频](#视频)部分观看我们的"Alluxio新贡献者"视频！

### Java 7或者8

开发Alluxio需要Java 7或者8，如果你不确定你系统上的Java版本，可以运行以下命令确认：

```bash
$ java -version
```

如果你还未安装Java，从[Java SDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)下载并安装。

### Maven

Alluxio项目使用Maven来管理构建，如果你还未安装，可以先[在这里下载Maven](https://maven.apache.org/download.cgi)，并按照
[Maven官方文档](https://maven.apache.org/install.html)进行安装。

### Git

Alluxio使用Git分布式版本控制系统来管理源码，因此需要安装Git。

如果你还未安装`git`，请先进行[安装](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)。

### GitHub账户

Alluxio源码托管在GitHub上，仓库地址为[Alluxio repository](https://github.com/Alluxio/alluxio)。

你需要一个GitHub账户来贡献源码，如果还没有，请先[注册一个GitHub账户](https://github.com/)。

另外你还需要知道[你的GitHub账户绑定了哪个邮箱](https://help.github.com/articles/adding-an-email-address-to-your-github-account/)。

### Jira账户

Alluxio开源项目使用[JIRA](https://alluxio.atlassian.net/projects/ALLUXIO/issues)来跟踪特性、bug以及问题。请[创建一个Alluxio JIRA账户](https://alluxio.atlassian.net/admin/users/sign-up)，从而能够新建、跟踪以及修复ticket。

请[创建一个Alluxio JIRA账户](https://alluxio.atlassian.net/admin/users/sign-up)来新建tickets,跟踪tickets以及为tickets提供修复。如果不能通过上述链接建立JIRA账户，请按下述格式发送邮件至jira-admin@alluxio.org：

```
Subject: JIRA Account Request
Body:
	Email Address: example@example.com
	Username: JohnS
	Full Name: John Smith
```

## Fork Alluxio源代码库

在向Alluxio贡献源码之前，你首先需要fork Alluxio源代码库。如果你还未这么做，先进入[Alluxio仓库](https://github.com/Alluxio/alluxio)，再点击页面右上角的Fork按钮， 之后你便有了Alluxio源代码库的fork了。

在fork Alluxio源代码库后，你需要从该fork创建一个本地的副本，这会将该fork里的文件拷贝到你的本地电脑。使用以下命令创建副本：

```bash
$ git clone https://github.com/YOUR-USERNAME/alluxio.git
$ cd alluxio
```

这会将副本拷贝在`alluxio/`目录下。

为了将远程的Alluxio源码改动更新到你本地的副本中，你需要创建一个指向远程Alluxio源代码库的源。在刚拷贝的副本的目录下，运行：

```bash
$ git remote add upstream https://github.com/Alluxio/alluxio.git
```

运行以下命令可以查看远程仓库的url：

```bash
$ git remote -v
```

这会显示`origin`（你的fork）以及`upstream`（Alluxio仓库）的url。

## 配置Git邮箱

在向Alluxio提交commit之前，你需要先确认你的Git邮箱设置正确，请参考该[邮箱设置指南](https://help.github.com/articles/setting-your-email-in-git/)。


## 编译Alluxio

既然你已经有了Alluxio源码的一个本地副本，那现在就可以编译Alluxio啦！

在本地副本目录下，运行以下命令编译Alluxio：

```bash
$ mvn clean install
```

该命令会编译整个Alluxio，并且运行所有测试，这可能会花费几分钟。

如果你仅仅只需要重新编译，而不需要运行检查和测试，可以运行：

```bash
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
```

这应该用不到一分钟。

这里有更多[编译Alluxio的细节](Building-Alluxio-Master-Branch.html)。

## 领取一个New Contributor JIRA Ticket

Alluxio的ticket中有许多不同的等级，它们分别是：**New Contributor**、**Beginner**、**Intermediate**、 **Advanced**。新的开发者应该在进行更高级的任务之前先完成两个**New Contributor**任务。**New Contributor**任务非常容易，不需要了解过多的关于代码的细节；**Beginner**任务通常只需要修改一个文件；**Intermediate**任务通常需要修改多个文件，但都在同一个包下；**Advanced**任务通常需要修改多个包下的多个文件。

我们鼓励所有新的代码贡献者在进行更难的任务之前先完成两个**New Contributor**任务，这是帮助你熟悉向Alluxio贡献源码的整个流程的好方法。

请浏览任何的未关闭的[New Contributor Alluxio Tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20NewContributor%20AND%20assignee%20in%20(EMPTY))并找到一个还未被他人领取的任务，你可以点击**Assign to me**链接来领取该ticket。另外你应该在你开始进行该任务之前先领取它，从而其他人知道你在完成这个ticket。

注意所有的Alluxio JIRA ticket都名为**ALLUXIO-####**，其中**####**是一个数字。当你创建一个pull request的时候，这个名称就非常重要了，这个pull request的标题应该像**[ALLUXIO-XXXX] Awesome Feature**这样。


## 在本地副本中创建一个新的分支

在领取ticket之后，切换到终端下，进入本地副本的目录，现在，我们开始动手修复吧！

要向Alluxio提交一个修改，最好是为每一个ticket单独建一个分支，并且在该分支下进行修改。以下命令将展示如何创建一个新的分支。

首先，确保你在本地副本的`master`分支下，运行以下命令切换到`master`分支：

```bash
$ git checkout master
```

接着，你应当确保你的`master`分支里的代码与最新的Alluxio源码同步，可以通过以下命令获取所有的代码更新：

```bash
$ git pull upstream master
```

这将会获取到Alluxio项目中所有的更新，并合并到你的本地`master`分支里。

现在，你可以新建一个分支来进行之前领取的**New Contributor**啦！运行以下命令创建一个名为**awesome_feature**的分支：

```bash
$ git checkout -b awesome_feature
```

这会创建该分支，并且切换到该分支下。现在，你可以修改相应的代码来处理该JIRA ticket啦！

## 提交本地commit

在你处理该ticket任务时，可以为修改的代码提交本地的commit，这在你完成了一个阶段性的修改时特别有用。运行以下命令将一个文件标记为准备提交阶段：

```bash
$ git add <file to stage>
```

一旦所有需要的文件都进入准备提交阶段后，可以运行以下命令提交包含这些修改的一个commit：

```bash
$ git commit -m "<concise but descriptive commit message>"
```

如果想了解更多信息，请参考该[commit提交指南](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository)。

## 提交一个Pull Request

在你完成了修复该JIRA ticket的所有修改后，就马上要向Alluxio项目提交一个pull request啦！这里是一个[提交一个pull request的详细指南](https://help.github.com/articles/using-pull-requests/)，不过以下是通常的做法。

在你提交了所有需要的本地commit后，你可以将这些commit推送到你的GitHub源代码库中。对于**awesome_feature**分支，运行以下命令推送到GitHub上：

```bash
$ git push origin awesome_feature
```

这将会把你本地的**awesome_feature**分支下的所有commit推送到你的GitHub上Alluxio fork的**awesome_feature**分支中。

一旦将所有的修改推送到了fork中后，访问该fork页面，通常上面会显示最近的修改分支，如果没有，进入你想提交pull request的那个分支上，再点击**New Pull Request**按钮。

在新打开的**Open a pull request**页面中，base fork应该显示为`Alluxio/alluxio`，并且base branch应该为**master**，head fork为你的fork，并且compare branch应该是你想提交pull request的那个分支。

对于这个pull request的标题，它应该以这个JIRA ticket名称为前缀，因此，这个标题应该像**[ALLUXIO-1234] Awesome Feature**这样（在标题里面，请用和你的request相关的信息替换掉 Awesome Feature，例如，"Fix format in error message"或者"Improve java doc of method Foo"）。

在描述框的第一行，请添加一个该JIRA ticket的链接，该链接像`https://alluxio.atlassian.net/browse/ALLUXIO-####`这样。

完成以上步骤后，点击下方的**Create pull request**按钮。恭喜！你向Alluxio的第一个pull request成功提交啦！！


## 审阅Pull Request

在pull request成功提交后，可以在[Alluxio源代码库的pull request页面](https://github.com/Alluxio/alluxio/pulls)看到它。

在提交后，社区里的其他开发者会审阅你的pull request，并且可能会添加评论或者问题。

在该过程中，某些开发者可能会请求你修改某些部分。要进行修改的话，只需简单地在该pull request对应的本地分支下进行修改，接着提交本地commit，接着推送到对应的远程分支，然后这个pull request就会自动更新了。详细操作步骤如下：

```bash
$ git add <modified files>
$ git commit -m "<another commit message>"
$ git push origin awesome_feature
```

在该pull request中的所有评论和问题都被处理完成后，审查者们会回复一个**LGTM**。在至少有两个**LGTM**后，一个管理员将会将你的pull request合并到Alluxio源码中。

祝贺！你成功地向Alluxio贡献了源码！非常感谢你加入到我们的社区中！！

## 视频

<iframe width="560" height="315" src="https://www.youtube.com/embed/QsbM804rc6Y" frameborder="0" allowfullscreen></iframe>

## 挑战更高级的任务

在你完成两个**New Contributor**任务之后，就可以尝试完成一些[Beginner Alluxio tickets](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20Beginner%20AND%20assignee%20in%20(EMPTY))了。

# 欢迎加入Alluxio社区！
