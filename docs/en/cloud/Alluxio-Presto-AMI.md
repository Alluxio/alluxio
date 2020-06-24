---
layout: global
title: Alluxio Presto Sandbox on AWS
nickname: AWS Presto Sandbox
group: Cloud Native
priority: 6
---

This guide describes how to launch a single node sandbox with Alluxio and Presto preconfigured to run queries.

* Table of Contents
{:toc}

## Overview

The Alluxio-Presto sandbox is an Amazon Machine Image offered on Amazon EC2
and features installations of MySQL, Hadoop, Hive, Presto, and Alluxio.

The goal of this guide is to show how Alluxio can improve Presto’s query performance
by reading through Alluxio to access locally cached data, originally stored in an Amazon S3 bucket.

## Prerequisites

- An account on [AWS](https://aws.amazon.com/)
- Web console access to [EC2](https://console.aws.amazon.com/ec2)
- A terminal window with [SSH](https://man.openbsd.org/ssh.1)

## Launch an EC2 instance with Presto + Alluxio stack

{% accordion launch %}
  {% collapsible Open the web console %}
Open a web browser and navigate to the [EC2 console](https://console.aws.amazon.com/ec2), logging in with your credentials when prompted.
Note the AWS region in the upper right corner.

![presto_sandbox_ec2_region]({{ '/img/presto_sandbox_ec2_region.png' | relativize_url }})

Click on the region to open a dropdown of available regions.
It is recommended to select the region that is geographically closest to your computer.

On the left sidebar, click the `Instances` link to arrive at the following page: 

![presto_sandbox_ec2_instances]({{ '/img/presto_sandbox_ec2_instances.png' | relativize_url }})

Click the blue `Launch Instance` button.
This will navigate you to the first of several steps to launch an instance.

  {% endcollapsible %}
  {% collapsible Choose an Amazon Machine Image (AMI) and Instance type %}
In the left sidebar, there are 4 categories: Quick Start, My AMIs, AWS Marketplace, and Community AMIs.
Click on `AWS Marketplace` and search for the Alluxio-Presto sandbox AMI by typing `alluxio-presto-sandbox` in the search bar.
Exactly one result should appear; click on its blue `Select` button to proceed.

![presto_sandbox_ec2_choose_ami]({{ '/img/presto_sandbox_ec2_choose_ami.png' | relativize_url }})

Note that each AMI has a unique ID in the form `ami-xxxxxxxxxxxxxxxxx`.
This ID differs depending on which AWS region was selected.
If you would like to launch the AMI with spot instances, click on `Community AMIs`
and among the 2 selections, choose the one with the shorter name.
The AMI with the longer name is derived from the shorter one but is constrained by the limitations imposed by AWS Marketplace.

Among the large table of instance types and their specifications, scroll down to search for the `r4.4xlarge` instance type.
If `r4.4xlarge` does not appear on the page, you may need to change the filters immediately above the table of instance types
from `Current generation` to `All generations`.

> **Warning:** `r4.4xlarge` is provisioned with the minimum amount of resources needed to run this AMI.
Choosing a different instance type may result in unexpected errors.

![presto_sandbox_ec2_instance_type]({{ '/img/presto_sandbox_ec2_instance_type.png' | relativize_url }})

Select it and click `6. Configure Security Group` in horizontal row of steps near the top.
Steps 3, 4, and 5 are skipped because we can use their default values. 

Note that the launched EC2 instance does not qualify for free usage tier because the instance needs to have sufficient resources to execute the workload.
The instance type we will be using, `r4.4xlarge`, costs only ~$1 an hour.

  {% endcollapsible %}
  {% collapsible Configure Security Group & Review Instance Launch %}

Create a new security group named `alluxio-presto-sandbox`.
Port 22 is already added to allow SSH access.
Click `Add Rule` on the bottom left to add another row.
Set the port range of the new rule to `8080` to allow access to the Presto web UI.
Set the source to be `Anywhere`.

Repeat the above steps to add another rule to open port `19999` to allow access to the Alluxio web UI.
Click the blue `Review and Launch` button on the bottom right to proceed.

![presto_sandbox_ec2_security_groups]({{ '/img/presto_sandbox_ec2_security_groups.png' | relativize_url }})

This page shows an overview of all the configurations set in the previous steps.
You can disregard the yellow warning boxes,
titled `Improve your instances' security` and `Your instance configuration is not eligible for the free usage tier`. 

![presto_sandbox_ec2_review]({{ '/img/presto_sandbox_ec2_review.png' | relativize_url }})

Click the blue `Launch` button on the bottom right, which will open a pop up regarding key pairs.

  {% endcollapsible %}
  {% collapsible Select an existing key pair or create a new key pair %}
If you have an existing key pair with its corresponding private key file,
keep `Choose an existing key pair` in the first dropdown and select the known key pair from the second dropdown.
Click the checkbox to acknowledge you have the private key file.

For new users without an existing key pair, select the second option `Create a new key pair`
and type in a name in the second text box (ex. `alluxio-presto-sandbox-keypair`).
Click `Download key pair`and your browser will download a private key file with a matching name (ex. `alluxio-presto-sandbox-keypair.pem`).
Keep track of where this file is downloaded (ex. `~/Downloads/alluxio-presto-sandbox-keypair.pem`)
because it will be used to access your launched instance.

![presto_sandbox_ec2_key_pair]({{ '/img/presto_sandbox_ec2_key_pair.png' | relativize_url }})

Click the blue `Launch Instances` button to finally launch the instance.
The browser will display a loading spinner with status messages
before arriving at a launched page with a blue `View Instances` button on the bottom.
Click this button to return back to the Instances page.

 > **Important for newer accounts!!**
Accounts may encounter an instance limit error when attempting to launch an instance of type `r4.4xlarge`.
This is because each account is limited on how many instances can be launched at a time for each type.
Newer accounts typically will not be allowed to launch larger instance types;
for example, the starting limit value for `r4.4xlarge` could be 0.

If you see the following error, follow the given URL to request for a limit increase. 

![presto_sandbox_ec2_limit_error]({{ '/img/presto_sandbox_ec2_limit_error.png' | relativize_url }})

Limit values can be found in the `Limits` page from the left sidebar. 

![presto_sandbox_ec2_limits]({{ '/img/presto_sandbox_ec2_limits.png' | relativize_url }})

  {% endcollapsible %}
  {% collapsible SSH into the instance %}
Navigate to the Instances page if not already open and find the entry for the newly launched instance.
Select the row and find the instance’s `Public DNS`; it should start with `ec2-` and end in `.amazonaws.com`.
This is the hostname used to SSH into the instance.

![presto_sandbox_ec2_launched_instance]({{ '/img/presto_sandbox_ec2_launched_instance.png' | relativize_url }})

Open a terminal window and use the `ssh` command to connect to the launched instance.
In the command below, replace:
- `/path/to/privateKeyFile.pem` with the path to your private key file
- `ec2-00-00-00-00.compute-1.amazonaws.com` with the EC2 instance public DNS

```console
$ ssh -i /path/to/privateKeyFile.pem ec2-user@ec2-00-00-00-00.compute-1.amazonaws.com
```

If you see an error labeled `WARNING: UNPROTECTED PRIVATE KEY FILE!`,
the permissions of the private key file needs to be updated.
Run `chmod 400 /path/to/privateKeyFile.pem`,
again replacing with the path to your private key file, and retry the SSH command again.

If this is your first time running SSH for this instance, you will be prompted to confirm the authenticity of the host.
Type `yes` to continue.

Once successfully connected, the terminal prompt should start with something similar to `[ec2-user@ip-00-00-00-00 ~]$`.
It is assumed for the remainder of this guide that commands will be run from within the instance.

  {% endcollapsible %}
{% endaccordion %}


## Explore Alluxio using its Web UI and CLI

{% accordion explore %}
  {% collapsible Use the Web UI and CLI %}
We’ll use a combination of the Alluxio web UI at **http://EC2_PUBLIC_DNS:19999**
and the [Alluxio CLI]({{ '/en/operation/User-CLI.html' | relativize_url }})
to explore the Alluxio filesystem and cluster status.

The instance comes with an Amazon S3 bucket pre-mounted in Alluxio at the `/s3` directory.
It contains data for [TPC-DS](http://www.tpc.org/tpcds/) benchmarks at the `scale 100` size factor
which amounts to about 38GB of data across multiple tables.

Open the Alluxio web UI at **http://EC2_PUBLIC_DNS:19999** to check if the Alluxio master has started successfully.
If not, wait a few moments, refresh the page, and it should become available.

You can see the current Alluxio mounts by running `alluxio fs mount` from within the instance.
```console
$ alluxio fs mount
/opt/alluxio/underFSStorage                           on  /    (local, capacity=15.99GB, used=-1B(0%), not read-only, not shared, properties={})
s3a://alluxio-public-http-ufs/tpcds/scale100-parquet  on  /s3  (s3, capacity=-1B, used=-1B, read-only, not shared, properties={aws.secretKey=******, aws.accessKeyId=******})
```

You can see the current Alluxio files from the Alluxio web UI or by running `alluxio fs ls /` from within the instance.
```console
$ alluxio fs ls /
drwxr-xr-x  ec2-user       ec2-user                     1       PERSISTED 07-17-2019 23:43:56:949  DIR /promotion
drwx------                                              4       PERSISTED 07-17-2019 23:45:16:220  DIR /s3
```

The `/s3` directory contains the remote data that is located in S3 (37GB)
and the `/promotion` directory contains data from a local file (53KB) that has already been cached in Alluxio worker memory.
This setup in Alluxio allows us to run a single Presto query that utilizes data from different sources.
  {% endcollapsible %}
{% endaccordion %}


## Run Queries with Presto on Alluxio

Now we’ll use Presto + Alluxio to show how Alluxio can massively decrease query times by reading cached data.
You’ll use Presto through the command line;
however, you can also use the Presto UI at **http://EC2_PUBLIC_DNS:8080** to view the status of your queries.
Note that the last step `Rerunning the Query` will show you the performance results.

{% accordion queries %}
  {% collapsible Open Presto CLI with alluxio schema %}
From within the instance, launch the Presto CLI:
```console
presto --catalog hive --debug
presto>
```

> **Tip:** You can exit at any time by typing `exit;`

The instance comes pre-loaded with tables in Presto. A schema named `alluxio` has already been defined.
The database contains the tables from the TPC-DS benchmark.
```console
presto> show schemas;
       Schema
--------------------
 alluxio
 default
 information_schema
(3 rows)
```

Use the `alluxio` schema by entering `use alluxio;`
```console
presto> use alluxio;
```

Once you see the prompt `presto:alluxio>` you will be using the schema.
The table definitions can be found in `/usr/share/tpcdsData/createAlluxioTpcdsTables.sql`
and `/usr/share/tpcdsData/createAlluxioPromotionTable.sql`.

  {% endcollapsible %}
  {% collapsible Run query %}
We’re going to run a query derived from the TPC-DS benchmarks that compares the profit between store and website channels.
Note again that this query utilizes data from remote S3 and a local file.
The full query below can also be found at `/usr/share/tpcdsData/prestoQuery.sql`.
```sql
with ssr as
(select  ss_store_sk as store_sk,
     sum(ss_ext_sales_price) as sales,
     sum(coalesce(sr_return_amt, 0)) as returns,
     sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
 from store_sales left outer join store_returns on
     (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number)
     , promotion
 where ss_promo_sk = p_promo_sk
 group by ss_store_sk),
wsr as
(select  ws_web_site_sk as website_sk,
     sum(ws_ext_sales_price) as sales,
     sum(coalesce(wr_return_amt, 0)) as returns,
     sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
 from web_sales left outer join web_returns on
     (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number)
     , promotion
 where ws_promo_sk = p_promo_sk
 group by ws_web_site_sk)
 select channel, sk, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit
 from (select 'store channel' as channel, store_sk as sk, sales, returns, profit from ssr
     union all
     select 'web channel' as channel, website_sk as sk, sales, returns, profit from  wsr) x
 group by rollup (channel, sk) order by channel, sk limit 100;
```

Copy the above query, paste it into the Presto prompt, and hit enter to execute.
The query will likely take at least 5 minutes to finish,
so it would be a good time to grab a drink from the fridge.
This may take longer depending on where your instance's region is relative to us-east-1,
which is where the s3 bucket hosting the raw data is located.

An output table is shown when done; the following is a sample of the first few rows:
```console
   channel    | sk  |        sales         |       returns        |        profit
---------------+-----+----------------------+----------------------+-----------------------
 store channel |   1 |  2.515118199869996E9 | 1.2543365919999991E8 | -1.1656864820600054E9
 store channel |   2 | 2.5205456842599974E9 |  1.245415812100001E8 | -1.1674391653999991E9
 store channel |   4 | 2.5179203736600065E9 | 1.2488394164999989E8 | -1.1657134956600013E9
 store channel |   7 |  2.511532314150002E9 | 1.2449183458999994E8 | -1.1649937964900005E9
 store channel |   8 | 2.5347543900099897E9 | 1.2612198162999986E8 | -1.1755503999099977E9
 store channel |  10 |      2.51036513155E9 | 1.2575692749000001E8 |  -1.160484859049994E9
 store channel |  13 |  2.513131863500003E9 | 1.2527703745000003E8 | -1.1657150094600005E9
 store channel |  14 |  2.535310933710003E9 |  1.264888773999999E8 | -1.1709913304899998E9
 store channel |  16 | 2.5312767249400077E9 |       1.2517431134E8 |  -1.168813352409999E9
```

Press `q` to leave the query results.

After quitting the results, the query summary should resemble the following:
```console
Query 20190719_001828_00003_vy8rx, FINISHED, 1 node
http://localhost:8080/ui/query.html?20190719_001828_00003_vy8rx
Splits: 8,164 total, 8,164 done (100.00%)
CPU Time: 1220.3s total,  325K rows/s, 5.17MB/s, 28% active
Per Node: 1.8 parallelism,  574K rows/s, 9.14MB/s
Parallelism: 1.8
Peak Memory: 2.17GB
11:30 [396M rows, 6.16GB] [574K rows/s, 9.14MB/s]
```

> **Note:** In the last line of the output, `11:30` represents the query time in `mm:ss` format.

  {% endcollapsible %}
  {% collapsible Rerun the query to see performance improvement %}
Because this is the first time we’re reading the data, it is pulled from S3 and is then returned through Alluxio.
At the same time, the Alluxio worker will be caching the data in memory so that the next time the data is accessed it can be read at memory speed.

Running the query again should be faster since the data is now cached in Alluxio,
unlike the first query which read its data from the S3 bucket.
Let’s run it again!

> **Tip:** You can hit the up arrow while in the Presto CLI to scroll through previously executed commands.
Exit the Presto shell with `exit;`

Take note of the query execution time; did you notice any difference in performance?
If you want replicate the same result, you will need to free the data from Alluxio memory.
After exiting the Presto shell,
run the alluxio [free]({{ '/en/operation/User-CLI.html#free' | relativize_url }})
command to free the data.

```console
$ alluxio fs free /s3
```

To terminate the SSH session, type `exit` in the SSH terminal prompt.

  {% endcollapsible %}
{% endaccordion %}

When complete, terminate the instance to prevent further charges to your AWS account.
