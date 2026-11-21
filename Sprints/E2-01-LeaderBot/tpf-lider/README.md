# tpf-lider

Base do líder do arquivo: um `LeaderBot` em Java (JDA) que regista os comandos no servidor
Discord de teste e responde com mensagens provisórias. Requer Java 17+ e Maven.

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

O `bot.properties` é procurado na pasta de trabalho, por isso, no VS Code, abrir diretamente a
pasta `tpf-lider`. Quando tudo corre bem, a consola mostra `7 comandos registados em <servidor>`.

## Comandos

| Comando | Argumentos | Só `admin-arquivo` |
| --- | --- | --- |
| `/registar` | `texto` | |
| `/estado` | | |
| `/config r` | `n` (≥ 1) | sim |
| `/recuperar` | `de` (≥ 0), `ate` (≥ `de`) | |
| `/replica-token` | | sim |
| `/replica-remover` | `id` | sim |
| `/rajada` | `n` (≥ 1), `intervalo-ms` (≥ 0) | sim |

O Discord só aceita nomes de opções em minúsculas, por isso `intervaloMs` chama-se `intervalo-ms`.
