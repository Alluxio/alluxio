---
layout: global
title: Rodando Localmente o Alluxio
nickname: Alluxio na Máquina Local
group: User Guide
priority: 1
---

# Rodar o Aluxio Standalone em um Única Máquina.

O pré-requisito para isto é que você possua o [Java](Java-Setup.html) (JDK 7 ou superior).

Baixe a distribuição do binário do Alluxio {{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

Antes de executar os `scripts` de inicialização, as variáveis de ambientes devem estar especificadas
em `conf/alluxio-env.sh`, este arquivo pode ser copiado a partir do arquivo modelo:

{% include Running-Alluxio-Locally/bootstrap.md %}

Para executar em modo `standalone`, confirme que:

* `ALLUXIO_UNDERFS_ADDRESS` dentro do `conf/alluxio-env.sh` está configurado para um diretório temporário
no `file system` local (exemplo, `export ALLUXIO_UNDERFS_ADDRESS=/tmp`).

* O serviço de autenticação deve estar configurado para que `ssh localhost` possa obter sucesso.

Posteriormente, você pode formatar e iniciar o `Alluxio FileSystem`. *Nota: Como o Alluxio precisa configurar
o [RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt), inicializar um
sistema local necessita que o usuário informe a senha do `root` para usuários Linux. Para contornar esta
necessidade, você pode adicionar a chave pública `ssh` para o servidor local dentro de
`~/.ssh/authorized_keys`.*

{% include Running-Alluxio-Locally/Alluxio-format-start.md %}

Para verificar que o Alluxio está em execução, você pode acessar
**[http://localhost:19999](http://localhost:19999)**, ou analisar os registros dentro da pasta `logs`.
Você também pode executar um teste:

{% include Running-Alluxio-Locally/run-sample.md %}

Na primeira execução do teste, você irá visualizar algo semelhante ao exposto a seguir:

{% include Running-Alluxio-Locally/first-sample-output.md %}

Você pode acessar novamente a interface de usuário `web` do Alluxio em
**[http://localhost:19999](http://localhost:19999)**. Clique em `Browse File System` na barra de
navegação e você deverá ver que os arquivos escritos no Alluxio efetuados pelo teste acima.

Para executar um teste de verificação mais completo:

{% include Running-Alluxio-Locally/run-tests.md %}

Você também pode para o Alluxio a qualquer tempo executando:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
