package rep04;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final Path MONITOR_FILE = Paths.get("monitor-produtor.txt");
    private static HeartbeatMonitor[] detetores;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: java rep04.Producer <N> [api]");
            System.exit(1);
        }

        int n;
        try {
            n = Integer.parseInt(args[0]);
            if (n < 1) {
                System.err.println("Erro: O número de partições (N) tem de ser maior ou igual a 1.");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("Erro: O número de partições (N) deve ser um inteiro válido.");
            System.exit(1);
            return;
        }

        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        Random random = new Random();

        iniciarDetetores(ps, n);

        System.out.println("Produtor iniciado (N = " + n + ", API = " + api
                + ", Δ=" + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");
        System.out.println("Comandos aceites:");
        System.out.println("  <sensor> <temperatura> (ex: S01 22.5)");
        System.out.println("  rajada <n> <s>");
        System.out.println("  rajada-quente <n> <s>");
        System.out.println("Ctrl+C para terminar.");

        while (true) {
            System.out.print("> ");
            String linha = stdin.readLine();
            if (linha == null) {
                break; // Ctrl+D
            }

            linha = linha.trim();
            if (linha.isEmpty())
                continue;

            String[] partes = linha.split("\\s+");

            if (partes[0].equals("rajada") && partes.length == 3) {
                try {
                    int qtd = Integer.parseInt(partes[1]);
                    int maxSensores = Integer.parseInt(partes[2]);

                    if (qtd < 1 || maxSensores < 1) {
                        System.err
                                .println("Erro: A quantidade e o número de sensores devem ser maiores ou iguais a 1.");
                        continue;
                    }

                    executarRajada(ps, partitioner, n, qtd, maxSensores, random, false);
                } catch (NumberFormatException e) {
                    System.err.println("Erro: Argumentos numéricos inválidos para a rajada.");
                }
            } else if (partes[0].equals("rajada-quente") && partes.length == 3) {
                try {
                    int qtd = Integer.parseInt(partes[1]);
                    int maxSensores = Integer.parseInt(partes[2]);

                    if (qtd < 1) {
                        System.err.println("Erro: A quantidade deve ser maior ou igual a 1.");
                        continue;
                    }
                    if (maxSensores < 2) {
                        System.err.println("Erro: O comando rajada-quente exige pelo menos 2 sensores (s >= 2).");
                        continue;
                    }

                    executarRajada(ps, partitioner, n, qtd, maxSensores, random, true);
                } catch (NumberFormatException e) {
                    System.err.println("Erro: Argumentos numéricos inválidos para a rajada-quente.");
                }
            } else if (partes.length == 2) {
                try {
                    String sensor = partes[0];
                    double temp = Double.parseDouble(partes[1]);
                    publicarLeitura(ps, partitioner, n, sensor, temp);
                } catch (NumberFormatException e) {
                    System.err.println("Erro: Temperatura inválida.");
                }
            } else {
                System.err.println("Comando inválido. Use: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s>");
            }
        }
    }

    // um detetor por partição, alimentado apenas pelo cabeçalho dos LOTE
    private static void iniciarDetetores(IpfsPubSub ps, int n) {
        detetores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            final int partition = k;
            detetores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, Producer::evento);
            Thread subscription = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), message -> {
                        try {
                            Batch lote = Batch.decodeHeader(message);
                            detetores[partition].onHeartbeat(lote.getInc(), lote.getHb());
                            // só para o ficheiro, para não encher a consola onde se escrevem os comandos
                            escrever(System.currentTimeMillis() + ";RECEBIDO;p" + partition
                                    + ";inc=" + lote.getInc() + ";hb=" + lote.getHb()
                                    + ";ultimoSeq=" + lote.getLastSeq() + ";n=" + lote.size() + ";seq=-");
                        } catch (Exception e) {
                            System.out.println("REJEITADO: " + message + " (" + e.getMessage() + ")");
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Erro na subscrição p" + partition + ": " + e.getMessage());
                }
            }, "producer-hb-p" + partition);
            subscription.setDaemon(true);
            subscription.start();
        }

        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "producer-verificacao");
            t.setDaemon(true);
            return t;
        });
        verificacao.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor d : detetores) {
                d.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private static void avisarSeSuspeito(int k, String sensor) {
        if (detetores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }
    }

    // instante;evento;p<k>;detalhe, na consola e em monitor-produtor.txt
    private static void evento(String linha) {
        String l = System.currentTimeMillis() + ";" + linha;
        System.out.println(l);
        escrever(l);
    }

    private static synchronized void escrever(String l) {
        try {
            Files.writeString(MONITOR_FILE, l + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Erro ao escrever " + MONITOR_FILE + ": " + e.getMessage());
        }
    }

    private static void publicarLeitura(IpfsPubSub ps, Partitioner partitioner, int n, String sensor, double temp)
            throws Exception {
        int k = partitioner.of(sensor);
        String topico = Wire.topicIn(k);
        String mensagem = Wire.in(sensor, temp, 0, new VectorClock(n));

        avisarSeSuspeito(k, sensor);
        ps.publish(topico, mensagem);
        System.out.println("Enviado para " + topico + " (-> p" + k + ") -> " + mensagem);
    }

    private static void executarRajada(IpfsPubSub ps, Partitioner partitioner, int n, int qtd, int maxSensores, Random random,
            boolean quente) throws Exception {
        long inicio = System.currentTimeMillis();
        int[] contagem = new int[partitioner.count()];

        for (int i = 0; i < qtd; i++) {
            String sensor;

            if (quente && random.nextDouble() < 0.8) {
                sensor = "S01";
            } else {
                int min = quente ? 2 : 1;
                int numSensor = min + random.nextInt(maxSensores - min + 1);
                sensor = String.format("S%02d", numSensor);
            }

            double temp = 15.0 + (15.0 * random.nextDouble());
            int k = partitioner.of(sensor);

            avisarSeSuspeito(k, sensor);
            ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(n)));
            contagem[k]++;
        }

        long fim = System.currentTimeMillis();

        System.out.println("Rajada terminada!");
        System.out.println("Instante início: " + inicio);
        System.out.println("Instante fim: " + fim);
        System.out.println("Tempo total: " + (fim - inicio) + " ms");
        for (int k = 0; k < partitioner.count(); k++) {
            System.out.println("  p" + k + ": " + contagem[k] + " leituras");
        }
    }
}
