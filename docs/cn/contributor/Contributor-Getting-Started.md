---
layout: global
title: 开发指南
nickname: 开发指南
group: Contributor Resources
priority: 1
---

欢迎来到Alluxio社区，我们热烈欢迎你参与到我们的社区并且对社区做贡献。
本页面提供了如何参与到Alluxio开源项目社区并成为其中一员的指导。

* 目录
{:toc}

## 软件要求

最基本的需要是一台安装了Mac OS X或Linux的电脑，目前版本的Alluxio尚未对Windows系统提供支持。

我们建议您先按照基于源码编译Alluxio
[指南克隆并编译Alluxio源代码]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }})。

### 软件要求

- 所需软件：
  - Java 8
  - Maven 3.3.9+
  - Git

### 帐户准备

#### Github帐户

您需要有一个GitHub帐户才能向Alluxio代码仓库贡献代码。

您需要知道您的GitHub电子邮件地址才能进行贡献。 您可以在[个人资料电子邮件设置](https://github.com/settings/emails)中找到你的邮件地址。

### 配置您的Git电子邮件

在向Alluxio提交代码之前，您应该确认你的Git电子邮件设置正确。
[请访问有关设置电子邮件的说明。](https://help.github.com/articles/setting-your-email-in-git/).

## 分叉Alluxio代码仓库

为了向Alluxio贡献代码，您先需要分叉Alluxio代码仓库。 如果您还没分叉代码仓库，您可以访问[Alluxio代码仓库](https://github.com/Alluxio/alluxio)并按页面右上角的Fork按钮。 之后，您将拥有自己的Alluxio代码仓库分支。


分叉Alluxio代码仓库之后，应创建分叉代码仓库的本地克隆。 这会将您分叉的文件复制到您的计算机上。 您可以使用以下命令克隆您的分叉：

```console
$ git clone https://github.com/YOUR-USERNAME/alluxio.git
$ cd alluxio
```

这将在`alluxio/`目录下创建克隆。

要将更改从开源Alluxio原始代码库提取到克隆中，您需要创建一个指向Alluxio原始代码库的新远程代码存储库。 要添加新的远程代码存储库，请在新创建的克隆目录中运行：

```console
$ git remote add upstream https://github.com/Alluxio/alluxio.git
```

这将创建一个称为`upstream`的远程代码仓库指向Alluxio原始代码仓库。 您可以用以下命令查看远程代码仓库的URL: 

```console
$ git remote -v
```

这会显示远程代码仓库的URL，包括`origin`（您的分叉）和`upstream`（Alluxio原始代码仓库）。

## 编译Alluxio

您现在可以编译Alluxio！

在本地克隆目录中，使用以下命令编译Alluxio：


```console
$ mvn clean install
```

这将编译所有的Alluxio，并运行所有测试。取决于您的硬件，这可能需要几分钟到半小时才能完成。

如果您想重新编译而不运行所有检查和测试，运行：


```console
$ mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip \
  -Dcheckstyle.skip -Dlicense.skip
```

此编译所花时间应少于1分钟。

这里有更多
[编译Alluxio源代码的细节]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relativize_url }})。

## 领取一个New Contributor任务

Alluxio的ticket中有许多不同的等级，它们分别是：**New Contributor**、**Beginner**、**Intermediate**、 **Advanced**。
新的开发者应该在进行更高级的任务之前先完成两个**New Contributor**任务。
**New Contributor**任务非常容易，不需要了解过多的关于代码的细节；**Beginner**任务通常只需要修改一个文件；**Intermediate**任务通常需要修改多个文件，但都在同一个package下；**Advanced**任务通常需要修改多个package下的多个文件。


我们建议所有新的贡献者在执行更高级的任务之前，先解决两个**New Contributor**任务。这是使自己熟悉为Alluxio项目做贡献的整个过程的好方法。

看一些在“Open”状态的[“新的Contributor Alluxio任务”](https://github.com/Alluxio/new-contributor-tasks/issues)，然后找一个没分配的任务。为了自我分配或接收调查表，请在问题中留下评论，例如`/assign @yourUserName`，以表明您正在处理该问题。
在开始解决问题单之前，您应该将问题单分配给自己，以便社区中的其他人知道您正在处理和完成问题单。

注意所有Github Issue都有一个对应的数字。当你创建一个Pull Request的时候，请在该Issue的描述中加入如下某一种描述
 * "Fixes Alluxio/new-contributor-tasks#123"，
 * "Fixed Alluxio/new-contributor-tasks#123"，
 * "Fix Alluxio/new-contributor-tasks#123"，
 * "Closes Alluxio/new-contributor-tasks#123"，
 * "Closed Alluxio/new-contributor-tasks#123" 
 * "Close Alluxio/new-contributor-tasks#123"。


### 在克隆中创建分支

找到一个任务后，回到终端并转到本地克隆的目录下。 
您可以开始解决问题了！

为了提交更改给Alluxio，最佳实践是在一个单独分支中完成对您针对一个问题的所有更改。以下内容介绍如何创建分支。

首先，确保您在您克隆的`master`分支中。运行以下命令切换到`master`分支：

```console
$ git checkout master
```

然后，您应确保您的master分支与不断发展的Alluxio原始代码库中的最新更改保持同步。运行以下命令来提取项目中所有最新更改：

```console
$ git pull upstream master
```

这将会从`upstream`仓库的`master`分支中获取到所有的更新，并合并到你的本地`master`分支里。

现在，您可以创建一个新分支来处理您之前领取的**New Contributor**任务。创建分支名称为**awesome_feature**s，运行：


```console
$ git checkout -b awesome_feature
```

这将创建分支，并切换到该分支。您可以修改必要的代码以解决该问题。

### 创建本地提交


在你处理该任务时，您可以创建代码的本地提交。 当您完成一部分很明确的阶段性修改后，这将很有用。 运行以下命令将一个文件标记为准备提交阶段：


```console
$ git add <file to stage>
```

在所有合适的文件都准备好提交之后，可以运行以下命令提交包含这些修改的一个commit：

```console
$ git commit -m "<concise but descriptive commit message>"
```

请阅读[Alluxio编码]({{ '/en/contributor/Code-Conventions.html' | relativize_url }})惯例以获取更多详细信息和有关如何更新Alluxio源代码的提示。

 
如果您想要更多有关创建提交的信息，请访问有关如何创建提交的[说明](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository)。

### 提交一个Pull Request (PR)

完成所有解决问题所需修改后，您可以向Alluxio项目提交Pull Request了！这是[有关提交Pull Request的详细说明](https://help.github.com/articles/using-pull-requests/)，但是以下是通常做法。

创建所有必要的本地提交后，可以将所有提交推送到你在GitHub中的代码仓库中。针对您的**awesome_feature**分支，可以使用以下命令推送到GitHub：

```console
$ git push origin awesome_feature
```

这会将您在本地分支**awesome_feature**中的所有新commit，推送到您的Alluxio分支中的GitHub中的**awesome_feature**分支中。

将所有更改推送到分支后，请访问您的Alluxio的GitHub分支。通常，Github会显示您的哪个分支最近已更新，但如果未更新，直接选择要提交Pull Request的分支（在本示例中为**awesome_feature**），然后按新Pull Request按钮。

在提交新Pull Request页面中，基础分支应为Alluxio/alluxio，基础分支应为`master`。头分支将是您的分支，而compare分支应是您要提交Pull Request的分支（在此示例中为awesome_feature）。

#### Pull Request标题

使用有效的Pull Request标题很重要。这里有一些非常好的PR标题的提示和惯例。这些提示和窍门是从现有非常好的
[提交信息规则中改编而成的。](https://chris.beams.io/posts/git-commit/#seven-rules)

* 标题不能太长（<〜50个字符），也不能太短（描述清楚）
* 标题应以祈使动词开头
  * 例子：修复Alluxio UI问题，重构Inode缓存逻辑
  * 错误例子：~~修复了Alluxio UI错误~~，~~ Inode缓存重构~~
  * [标题允许的开始单词列表](https://github.com/Alluxio/alluxio/blob/master/docs/resources/pr/pr_title_words.md)
* 标题的第一个单词应大写（英文）
* 标题不应以句号结尾

这些规则有一些例外。 标题开头前可以加前缀。 前缀全大写，并与标题的其余部分用一个空格隔开。 下面是一些可用前缀。
* **[DOCFIX]**: 这是为更新文档的PRs
  * 例子：‘**[DOCFIX]**更新入门指南’，‘**[DOCFIX]**添加GCS文档‘
* **[SMALLFIX]**：用于很小不会更改任何逻辑的PRs修改，例如拼写错误
  * 示例：‘**[SMALLFIX]**修复AlluxioProcess中的错字’，‘**[SMALLFIX]**改进GlusterFSUnderFileSystem中的注释格式’

#### Pull Request说明

写一个好的PR描述也很重要。 以下是有关非常好的PR描述的一些提示和惯例，这些提示和窍门是从现有非常好的
[提交信息规则中改编而成的](https://chris.beams.io/posts/git-commit/#seven-rules)。

* 描述此PR更改的内容以及更改原因
* 描述应包括此更改的任何正面和负面影响
* 描述中的各段应以空白行分隔
* 如果此Pull Request解决一个Github问题，请在描述框的**最后**一行添加指回该问题的链接。
  * 如果此PR解决了Github问题＃1234，则在Pull Request描述的最后包括'Fixes＃1234'，'Fixed＃1234'，'Fix＃1234'，'Closes＃1234'，'Closed＃1234'或'Close＃1234'。
  * 如果问题来自New Contributor任务，请在版本号＃1234之前添加代码仓名称'Alluxio / new-contributor-tasks'，例如'Fixes Alluxio / new-contributor-tasks＃1234。'

完成所有操作后，单击**“创建Pull Request”**按钮。 恭喜你！ 您对Alluxio的第一个Pull Request已提交！

### 审阅Pull Request

Pull Request提交后，您可以在Alluxio代码仓库的[Pull Request](https://github.com/Alluxio/alluxio/pulls)页面上找到它。


提交后，社区中的其他开发人员将审核您的Pull Request。 其他人可能会在您的Pull Request中添加评论或问题。

在代码审阅期间，请回复审阅者留下的所有评论，以便审阅者知道已解决了哪些评论以及每个评论是如何解决的。

在此过程中，有些人可能会要求修改部分Pull Request。 为此，您只需要在用于该Pull Request的分支中进行更改，创建一个新的本地提交，推送到您的远程分支，该Pull Request将自动更新。 详细步骤：

```console
$ git add <modified files>
$ git commit -m "<another commit message>"
$ git push origin awesome_feature
```

在Pull Request处理和解决了所有评论和问题后，审阅者将为您的Pull Request**LGTM**回复并批准Pull Request。 在至少一个批准后，一个维护人员将把您的Pull Request合并到Alluxio代码库中。

恭喜你！ 您已成功为Alluxio做出了贡献！ 感谢您加入社区！

## Video

<iframe width="560" height="315" src="https://www.youtube.com/embed/QsbM804rc6Y" frameborder="0" allowfullscreen></iframe>

## 提交新特性

如果您有一个非常好的关于新的Alluxio特性想法，我们强烈建议您实施并贡献到Alluxio代码仓库。

1. 在[Alluxio代码仓库](https://github.com/Alluxio/alluxio/issues)中创建有关您的特性的Github问题。 
1. 在Github问题中遵循这个[模板]({{ '/resources/templates/Design-Document-Template.md' | relativize_url }})附加特性设计文档。设计文档应遵循模板，并提供有关每个部分的信息。我们建议使用公开的Google文档，以进行更有效的协作和讨论。如果您不希望使用Google文档，则可以选择附加Markdown文件或PDF文件设计文档。
1. 标记您的问题为'type-feature'。 Alluxio成员定期检查所有未解决的问题，并将分配审阅者们来审阅设计文档。社区用户无权在Alluxio代码仓库中分配问题。因此，如果您想启动审阅过程，则可以创建一个占位Pull Request，链接到该问题，附加特性设计文档，并在那里向Alluxio成员请求审阅。
1. 审阅者（相应的Github问题的责任人或Pull Request的审阅者）将审阅特性设计文档，并提供反馈或迭代修改请求。如果您不确定特定的设计决策，请随时提出问题并寻求审阅者和社区成员的帮助。还请列出该特性的可能选项以及相应的利弊。
1. 在对设计文档进行几次迭代修改之后，审阅者将为设计文档提供LGTM。此功能就可以进入实施阶段。
1. 如上一节所述，创建一个Fork Alluxio代码仓库，实现您的特性并创建一个Pull Request。另请在您的Pull Request中链接到问题和设计文档。在Pull Request中，您还应该将有关特性的文档添加到[Alluxio文档中](https://docs.alluxio.io/)。
1. 在您的Pull Request被审核且合并后，您已向Alluxio提供了新特性。干杯祝贺!

## 下一步

新的贡献者可以通过以下一些资源来进一步熟悉Alluxio：

1.  [本地运行Alluxio]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})
1.  [在集群上运行Alluxio]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }})
1.  读[配置设置和命令行界面]({{ '/cn/operation/Configuration.html' | relativize_url }}) 和
[命令行界面]({{ '/cn/operation/User-CLI.html' | relativize_url }})
1.  读[代码示例](https://github.com/alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)
1.  [从源代码编译Alluxio]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }})
1.  分叉存储库，为一个或两个文件添加单元测试或javadoc，提交Pull Request。也欢迎您在我们的Github问题中解决问题。这里是尚未分配的[新贡献者任务的列表](https://github.com/Alluxio/new-contributor-tasks/issues)。每个New Contributer请限制在2个任务。然后，尝试一些Beginner/Intermediate任务，或在[用户邮件论坛中询问](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)。相关教程，请参阅有关[分叉代码仓](https://help.github.com/articles/fork-a-repo)和[发送Pull Request的GitHub指南](https://help.github.com/articles/using-pull-requests)。


## 欢迎来到Alluxio社区！
