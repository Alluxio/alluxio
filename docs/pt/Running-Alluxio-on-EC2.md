---
layout: global
title: Rodando Alluxio no EC2
nickname: Alluxio no EC2
group: User Guide
priority: 3
---

Alluxio pode ser implementado no `Amazon EC2` utilizando 
[Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant) que vem junto 
com o Alluxio. Os `scripts` permitem criar, configurar e destruir `clusters` que podem ser 
configurados automaticamente com [Amazon S3](https://s3.amazonaws.com/).

# Pré-requisitos

**Instale o Vagrant e os AWS plugins**

Baixe o [Vagrant](https://www.vagrantup.com/downloads.html)

Instale o `AWS Vagrant plugin`:

{% include Running-Alluxio-on-EC2/install-aws-vagrant-plugin.md %}

**Instale o Alluxio**

Baixe o Alluxio para sua máquina local e descompacte:

{% include Common-Commands/download-alluxio.md %}

**Install python library dependencies**

Instale o [python>=2.7](https://www.python.org/), não instale o `python3`.

Dentro do diret[orio `deploy/vagrant` em sua instalação local do Alluxio, execute:

{% include Running-Alluxio-on-EC2/install-vagrant.md %}

Alternativamente, você pode instalar manualmente o [pip](https://pip.pypa.io/en/latest/installing/) 
e então dentro de `deploy/vagrant`, execute:

{% include Running-Alluxio-on-EC2/install-pip.md %}

# Iniciando um Cluster

Parar rodar um Alluxio `cluster` no `EC2`, primeiro você deve possuir uma conta na 
[Amazon Web Services site](http://aws.amazon.com/).

Se você não está habituado com o `Amazon EC2`, primeiro, você pode ser 
[este tutorial](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html).

Então crie as [chaves de acesso](https://aws.amazon.com/developers/access-keys/) e defina as 
variáveis de ambiente `shell` `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`:

{% include Running-Alluxio-on-EC2/access-key.md %}

Em seguida, gere suas `EC2` 
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). Tenha 
certeza de definir as permissões dos arquivos de chaves privadas que somente você pode ler:

{% include Running-Alluxio-on-EC2/generate-key-pair.md %}

Copie o arquivo `deploy/vagrant/conf/ec2.yml.template` para `deploy/vagrant/conf/ec2.yml`:

{% include Running-Alluxio-on-EC2/copy-ec2.md %}

No arquivo de configuração `deploy/vagrant/conf/ec2.yml`, defina o valor de `Keypair` para 
sua `keypair` e o caminho da chave `pem` para sua `Key_Path`.

Por padrão, o `Vagrant script` cria um 
[Grupo de Segurança](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
nomeado de *alluxio-vagrant-test* em 
[Region(**us-east-1**) e Availability Zones(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html).
O `security group` será criado automaticamente na região com todo os tráfegos abertos de 
`inbound/outbound network`. Você pode alterar o *security group*, *region* e *availability zone* no 
`ec2.yml`. A zona padrão pode estar indisponível por alguns instances.
Nota: o `keypair` está associado a uma região específica. Por exemplo, se você criar um `keypaur` em 
us-east-1, esta está inválida em outras regiões (como us-west-1). Se você tiver erros de 
permissão ou conexão, por favor, primeiro cheque a região/zona.

**Spot instances**

Utilizando `spot instances` é uma formar de reduzir custo com `EC2`. `Spot instances` são intâncias não 
garantidas que são precificadas com ofertas. Atente que `spot instances` podem ser retirados de você 
se alguém efetuar uma oferta maior e não existirem outras `spot instances` disponíveis. Entretanto, 
por curto tempo de teste, `spot instances` são muito apropriadas devido ser muito raro que estas 
sejam retiradas de você.

Por padrão, o `script` de implementação NÃO USA `spot instances`. Sendo assim, você tem que habilitar o 
uso de `spot instances` no `script` de implementação.

Para habilitar `spot instances`, você deve modificar o arquivo `deploy/vagrant/conf/ec2.yml`:

    Spot_Price: “X.XX”

Para `AWS EC2`, o `underfs` padrão é o `S3`. Você precisa logar em sua [console Amazon S3](http://aws.amazon.com/s3/), 
criar um `S3 bucket` e escrever o nome deste `bucket` no campo `S3:Bucket` em `conf/ufs.yml`. Para utilizar 
outro `under storage system`, configure o campo `Type` e as configurações correspondentes em `conf/ufs.yml`.

Agora você pode iniciar o Alluxio `cluster` com o seu `under filesystem` escolhido dentro 
`availability zone` através da execução do `script` em `deploy/vagrant`:

{% include Running-Alluxio-on-EC2/launch-cluster.md %}

Cado nó do `cluster` executa um Alluxio `worker` e o `AlluxioMaster` roda o Alluxio `master`.

# Acessando o cluster

**Acesso através da Web UI**

Depois que o comando `./create <number of machines> aws` tiver sucesso, você pode ver duas linhas 
verdes como as visualizadas abaixo no final do resultado do `shell`:

{% include Running-Alluxio-on-EC2/shell-output.md %}

A porta padrão para o Alluxio `Web UI` é **19999**.

Acesse `http://{MASTER_IP}:{PORT}` no seu navegador para ter acesso a `Web UI`.

Você também pode monitorar a instância através do [AWS web console](https://console.aws.amazon.com/console). 
Tenha certeza que você está na console da região que você iniciou o `cluster`.

Estes são os cenários onde você pode querer checar o console:
 - Quando a criação do `cluster` falhar, cheque o `status` e os `logs` da instância `EC2`.
 - Depois que o `cluster` estiver destruído, confirme que as instâncias `EC2` foram terminadas.
 - Quando você não precisar mais do `cluster`, tenha certeza que as instâncias `EC2` não estão mais custando dinheiro.

**Acessando com ssh**

Os nós são configurados com nomes como `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2` e assim por diante.

Para conectar via `ssh` em um nó, execute:

{% include Running-Alluxio-on-EC2/ssh.md %}

Por exemplo, você pode conectar via `ssh` para o `AlluxioMaster` com:

{% include Running-Alluxio-on-EC2/ssh-AlluxioMaster.md %}

Todo o `software` está instalado sobre o diretório raiz, exemplo, o Alluxio está instalado em 
`/alluxio`.

No nó `AlluxioMaster`, você pode executar testes sobre o Alluxio para checar a saúde do ambiente:

{% include Running-Alluxio-on-EC2/runTests.md %}

Depois que os testes terminarem, acesse novamente a interface `web` do Alluxio em 
`http://{MASTER_IP}:19999`. Clique em `Browse File System` na barra de navegação e você deverá 
ver arquivos escritos no Alluxio pelo teste acima.

Você pode logar no [AWS web console](https://console.aws.amazon.com/console), então ir até o 
`S3 console` e verificar os arquivos escritos no `S3 bucket` pelo teste acima.

A partir de um nó do `cluster`, você pode conectar via `ssh` com os outros nós do `cluster` 
sem precisar de senha:

{% include Running-Alluxio-on-EC2/ssh-other-node.md %}

# Destruindo o cluster

Sobre o diretório `deploy/vagrant`, você pode executar o comando abaixo para destruir o 
`cluster` que você criou:

{% include Running-Alluxio-on-EC2/destroy.md %}

Somente um `cluster` pode ser criado por vez. depois que o comando obtiver sucesso, as instâncias
`EC2` estarão terminadas.
