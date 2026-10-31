# COMINDIR04 · Como compilar e executar

Quatro projetos (`rep04-produtor`, `rep04-lider`, `rep04-replica`, `rep04-processador`), todos no pacote `rep04`.
`Batch` e `HeartbeatMonitor` são iguais nos quatro; `HeartbeatCheck` e `CausalCheck` estão em `rep04-replica`.

## Compilar (Git Bash, a partir desta pasta)

```bash
for p in lider replica processador produtor; do (cd rep04-$p && javac -encoding UTF-8 -d out *.java); done
```

## Executar (IPFS com `Pubsub.Enabled`, todos na mesma máquina e na mesma pasta de trabalho)

Ordem: réplicas → líderes → processador → produtor. Para mudar Δ ou o fator, passar o mesmo
`-Drep04.periodo=<ms>` / `-Drep04.fator=<f>` a **todos** os processos (e ao `HeartbeatCheck`).

```bash
java -cp ../rep04-replica/out rep04.PartitionReplica 1 0,1      # <id> <partições> [portoApi] [N=3]
java -cp ../rep04-replica/out rep04.PartitionReplica 2 1,2
java -cp ../rep04-replica/out rep04.PartitionReplica 3 2,0
java -cp ../rep04-lider/out rep04.PartitionLeader 0 3            # <k> <N> [endereço] [api]
java -cp ../rep04-lider/out rep04.PartitionLeader 1 3
java -cp ../rep04-lider/out rep04.PartitionLeader 2 3
java -cp ../rep04-processador/out rep04.AlarmProcessor 3
java -cp ../rep04-produtor/out rep04.Producer 3                  # S01 21.0 | rajada 300 10
```

Ficheiros produzidos: `lider-p<k>.txt`, `hb-p<k>.txt`, `replica-<r>-p<k>.txt`,
`monitor-replica<r>.txt`, `monitor-processador.txt`, `monitor-produtor.txt`.

## Provocar falhas (ponto 8)

```bash
echo "p1;$(date +%s%3N)" >> falhas.txt; taskkill //F //PID <pid do java do líder>   # kill -9 no Linux
```

A suspensão de 8(d) (`kill -STOP` / `kill -CONT`) não existe no Windows: usar o Monitor de Recursos
(botão direito no processo → *Suspender processo*) ou `pssuspend` da Sysinternals.

Falhas deliberadas do ponto 9 no líder: `-Drep04.falha=1..4` (1 só publica com pendentes,
2 heartbeat altera L e V, 3 `ultimoSeq` lido fora da secção sincronizada, 4 `ultimoSeq`=0 e descarta
um em cada dez lotes). A quinta é `-Drep04.fator=1` nos participantes.

## Análise

```bash
java -cp rep04-replica/out rep04.HeartbeatCheck 3 <pasta> [falhas.txt]   # também escreve relatorio-heartbeat.txt
java -cp rep04-replica/out rep04.CausalCheck 3 <pasta>
```
