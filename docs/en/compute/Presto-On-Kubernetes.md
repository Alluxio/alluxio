---
layout: global
title: Running Presto on Alluxio in Kubernetes
nickname: Presto on Kubernetes
group: Data Applications
priority: 8
---

Alluxio can be run on Kubernetes together with Presto in the same pods. This guide demonstrates how
to run a query on Presto fetching data from co-located Alluxio running in a Kubernetes environment.

* Table of Contents
{:toc}

## Overview

Presto running on Kubernetes can use Alluxio as the data access layer. This guide walks through an
example Presto query on Alluxio in Kubernetes.

## Prerequisites

- A Kubernetes cluster (version >= 1.8).

## Basic Setup

Prepare operator and CRD yaml to use the presto operator.

```bash
kubectl apply -f service_account.yaml
kubectl apply -f role.yaml
kubectl apply -f role_binding.yaml
kubectl apply -f presto_v1_crd.yaml
kubectl apply -f operator.yaml
kubectl apply -f alluxio_presto_v1_cr.yaml
```

Check status and identify coordinator pod id:
```bash
kubectl get pods
```

Exec into the coordinator
```bash
kubectl exec -it $(kubectl get pods | grep presto-coordinator-presto-alluxio | cut -d' ' -f1) /bin/bash
```

```bash
presto-cli
show catalogs;
show schemas in tpch;
select * from tpch.sf1.nation;
```