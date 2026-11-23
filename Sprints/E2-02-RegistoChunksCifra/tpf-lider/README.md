# tpf-lider

Líder do arquivo: um `LeaderBot` em Java (JDA). Desde o sprint E2-02 implementa o caminho de escrita
local: cada `/registar` e cada mensagem em `#arquivo` recebe um LSN, é gravado em `registos.log` com
fsync antes da resposta e entra no chunk aberto. O chunk é selado por tamanho ou por tempo,
serializado com padding, cifrado com uma DEK derivada da `master.key` e gravado em `selados/`.
`/estado`, `/config`, `/recuperar`, `/replica-token` e `/replica-remover` mantêm a resposta provisória
do sprint E2-01. Requer Java 17+ e Maven.

## No Discord

- Na aplicação do bot, em **Bot**, ativar o intent **Message Content**.
- Convidar o bot com os scopes `bot` e `applications.commands` e as permissões de ver canais,
  ler e enviar mensagens.
- O servidor tem de ter o canal `#arquivo` e o papel `admin-arquivo`.

## Configurar o token

Copiar `bot.properties.example` para `bot.properties` e preencher os dois valores:

| Chave | Valor |
| --- | --- |
| `DISCORD_TOKEN` | Token do bot, gerado no Developer Portal |
| `DISCORD_GUILD_ID` | ID do servidor (botão direito no servidor, *Copiar ID*, com o modo de programador ativo) |

Em alternativa, definir variáveis de ambiente com os mesmos nomes, que têm prioridade sobre o
ficheiro. O `bot.properties` está no `.gitignore` e nunca deve ser adicionado ao repositório.

## Arrancar

Na pasta `tpf-lider`:

```bash
mvn compile exec:java
```

O `bot.properties`, a `master.key` e os dados do líder ficam na pasta de trabalho, por isso, no
VS Code, abrir diretamente a pasta `tpf-lider`. Quando tudo corre bem, a consola mostra os
parâmetros, `registos.log: o próximo LSN é <n>` e `7 comandos registados em <servidor>`.

Para não voltar a registar os comandos em cada arranque, mudar `REGISTAR_COMANDOS` para `false` no
início do `LeaderBot`: a consola mostra `Registo dos comandos ignorado` e o Discord continua a mostrar
os comandos registados num arranque anterior.

Os parâmetros podem ser alterados com propriedades de sistema (no PowerShell, com aspas à volta de
cada `-D`):

```bash
mvn compile exec:java "-Dtpf.selagem=2000" "-Dtpf.chunkMax=8192"
```

| Propriedade | Constante em `Wire` | Por omissão |
| --- | --- | --- |
| `tpf.chunkMax` | `CHUNK_MAX_BYTES` | 65536 |
| `tpf.pad` | `PAD_BYTES` | 4096 |
| `tpf.selagem` | `SELAGEM_MS` | 5000 |

Um valor que não seja um inteiro positivo recusa o arranque.

## Comandos

| Comando | Argumentos | Só `admin-arquivo` | Neste sprint |
| --- | --- | --- | --- |
| `/registar` | `texto` | | regista e responde com o LSN |
| `/estado` | | | provisório |
| `/config r` | `n` (≥ 1) | sim | provisório |
| `/recuperar` | `de` (≥ 0), `ate` (≥ `de`) | | provisório |
| `/replica-token` | | sim | provisório |
| `/replica-remover` | `id` | sim | provisório |
| `/rajada` | `n` (≥ 1), `intervalo-ms` (≥ 0) | sim | injeta `n` registos sintéticos |

O Discord só aceita nomes de opções em minúsculas, por isso `intervaloMs` chama-se `intervalo-ms`.
As mensagens enviadas para `#arquivo` são registadas pelo mesmo método de `/registar`; as de bots,
as de outros canais e os avisos do próprio Discord são ignorados. Cada registo de `/rajada` tem um
texto de 500 caracteres, o que faz `/rajada 500 0` encher quatro chunks de 64 KB.

## Ficheiros do líder

| Ficheiro | Conteúdo |
| --- | --- |
| `master.key` | 32 bytes aleatórios, gerados no primeiro arranque com `SecureRandom` e só com leitura para o dono (`chmod 400` em Linux e macOS; no Windows, uma ACL com uma única entrada de leitura para o dono). Se existir com outro tamanho, o arranque é recusado. |
| `registos.log` | Uma linha por registo: `lsn;instante;autor;canal;texto`, com o texto em Base64 (UTF-8) e o instante em ms desde 1970. |
| `eventos-lider.txt` | Uma linha por evento: `instante;evento;detalhe`. |
| `selados/<chunkId>.bin` | O chunk cifrado (texto cifrado seguido da tag GCM). |
| `selados/<chunkId>.hdr` | O cabeçalho do chunk: `keyVersion;salt;nonce`. |

Todos estão no `.gitignore`. Os LSN começam em 1. No arranque, o líder lê `registos.log`, confirma
que os LSN são 1, 2, 3, … sem repetições nem saltos e continua a partir do último. Uma última linha
sem `\n` é uma escrita interrompida, que nunca foi confirmada ao utilizador, e é cortada; qualquer
outra irregularidade recusa o arranque. Como o autor e o canal ficam em claro nas linhas, um `;` ou
uma quebra de linha nesses campos é trocado por `_`.

Eventos que o líder regista neste sprint:

```text
<instante>;REGISTO;lsn=<lsn>;autor=<autor>;canal=<canal>
<instante>;SELADO;chunkId=<id>;lsnDe=<lsn>;lsnAte=<lsn>;registos=<n>;tamanho=<bytes>;bin=<bytes>;motivo=<tamanho|tempo>
<instante>;ERRO;selagem;lsnDe=<lsn>;lsnAte=<lsn>;causa=<exceção>
```

`tamanho` é o tamanho do chunk com padding (múltiplo de `PAD_BYTES`) e `bin` o tamanho do `.bin`,
que tem mais 16 bytes, os da tag GCM. `ERRO` só aparece se a selagem falhar (por exemplo, disco
cheio); os registos desse chunk continuam em `registos.log`.

## Formato binário do chunk

O chunk aberto é selado quando o seu tamanho serializado (sem padding) chega a `CHUNK_MAX_BYTES`,
incluindo o registo que o fez chegar lá, ou quando passam `SELAGEM_MS` desde o seu primeiro registo.
Ao selar, os registos são serializados assim, com inteiros em big-endian:

| Offset | Bytes | Campo |
| --- | --- | --- |
| 0 | 4 | magia `TPFC` em ASCII |
| 4 | 1 | versão do formato, `1` |
| 5 | 4 | `n`, número de registos (`int`) |
| 9 | … | `n` registos, pela ordem dos LSN |
| … | … | padding: bytes `0x00` até o total ser múltiplo de `PAD_BYTES` (nenhum, se já o for) |

Cada registo:

| Bytes | Campo |
| --- | --- |
| 8 | `lsn` (`long`) |
| 8 | `instante` (`long`, ms desde 1970-01-01T00:00Z) |
| 4 + a | comprimento do autor em bytes (`int`), seguido do autor em UTF-8 |
| 4 + c | comprimento do canal em bytes (`int`), seguido do canal em UTF-8 |
| 4 + t | comprimento do texto em bytes (`int`), seguido do texto em UTF-8 |

Um registo ocupa `28 + a + c + t` bytes e o chunk `9 + Σ(28 + a + c + t)` antes do padding. A
leitura inversa (`Chunk.ler`) lê a magia, a versão e `n`, reconstrói os `n` registos e ignora o que
sobra, que é padding.

## Cifra e formato do `.hdr`

Cada chunk é cifrado por `ChunkCrypto.seal` com um salt de 32 bytes e um nonce de 12 bytes novos:

```text
DEK     = HKDF-SHA256(master.key, salt, info = "tpf-chunk-v1"), 32 bytes (extração e um bloco de expansão)
aad     = "tpf-v1" || keyVersion (4 bytes) || salt || nonce, com keyVersion = 1
.bin    = AES-256-GCM(DEK, nonce, chunk com padding, aad), com a tag de 128 bits no fim
chunkId = SHA-256 do .bin, em hexadecimal (minúsculas)
```

O `.hdr` é uma linha de texto ASCII `keyVersion;salt;nonce`, com `keyVersion` em decimal e o salt e o
nonce em Base64, por exemplo:

```text
1;OIATInCXgk8uygzCjHrrjw1huivHv8YMpwocc7UrBVg=;D0nZhnGZ9WqTGQPN
```

Cada ficheiro é escrito num temporário (`.selagem.tmp`, fora de `selados/`), forçado para o disco e
movido com `ATOMIC_MOVE`; o `.hdr` é gravado antes do `.bin`, por isso um `.bin` tem sempre o seu
`.hdr`. `ChunkCrypto.open` verifica, por esta ordem, o hash contra o `chunkId`, a versão da chave e a
autenticação GCM, e lança uma exceção à primeira falha.

## Abrir os chunks selados

Sem ligar ao Discord, o líder abre os chunks com a `master.key`:

```bash
mvn -q compile exec:java "-Dexec.args=abrir"
mvn -q compile exec:java "-Dexec.args=abrir <chunkId>"
```

O primeiro abre todos os chunks de `selados/` e confere cada um com o seu evento `SELADO` e com
`registos.log` (`OK`, `DIFERE` ou `FALHA` com a exceção). O segundo lista os registos de um chunk.

## Testes

```bash
mvn test
```

`ChunkCryptoTest` (JUnit 5, só no âmbito de teste) cifra e decifra e confirma que `open` lança uma
exceção com um byte alterado no texto cifrado, com outro salt, nonce ou `keyVersion` e com outra
master key. Também confere o HKDF com o caso de teste 1 do RFC 5869 e a composição do aad.

## Verificar os critérios de aceitação

Com o líder a correr na pasta `tpf-lider`:

1. `/registar olá` responde `Registado com LSN <n>` e a linha já está no fim de `registos.log`
   (`Get-Content registos.log -Tail 1` ou `tail -n 1 registos.log`).
2. Uma mensagem em `#arquivo` recebe a resposta `Mensagem de <autor> registada com LSN <n>`; as de
   outros canais e as dos bots não aparecem em `registos.log`.
3. Parar e voltar a arrancar o líder: a consola mostra o próximo LSN, e o registo seguinte continua a
   numeração.
4. Um único registo dá um `SELADO` com `motivo=tempo` cerca de 5 s depois; `/rajada 500 0` dá vários
   `SELADO` com `motivo=tamanho`, todos com `tamanho` múltiplo de 4096.
5. O nome de cada `.bin` é o SHA-256 do seu conteúdo:

   ```powershell
   Get-ChildItem selados\*.bin | ForEach-Object { "{0} {1}" -f ((Get-FileHash $_ -Algorithm SHA256).Hash.ToLower() -eq $_.BaseName), $_.Name }
   ```

   ```bash
   cd selados && for f in *.bin; do [ "$(sha256sum "$f" | cut -d' ' -f1).bin" = "$f" ] && echo "OK $f" || echo "FALHA $f"; done
   ```

6. Nenhum ficheiro de `selados/` tem texto em claro: `Select-String -Path selados\* -SimpleMatch "olá"`
   ou `grep -r "olá" selados/` não devolvem nada.
7. `abrir` mostra `OK` em todos os chunks.
8. Alterar um byte de um `.bin` faz `abrir <chunkId>` falhar com `INTEGRIDADE`; mudar o salt ou o nonce
   no `.hdr` falha com `AEADBadTagException`; mudar a `keyVersion` para 2 falha com
   `versao de chave desconhecida`; com outra `master.key`, todos falham com `AEADBadTagException`.
   Para alterar e repor o byte 100 de um `.bin` (correr duas vezes):

   ```powershell
   $f = (Get-ChildItem selados\*.bin)[0].FullName; $b = [IO.File]::ReadAllBytes($f); $b[100] = $b[100] -bxor 1; [IO.File]::WriteAllBytes($f, $b)
   ```
