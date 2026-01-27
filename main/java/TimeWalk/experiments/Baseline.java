package edge2Time.experiments;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MPBaseline {

    private static final int ITERATION = 3;
    private static final double TD_JOIN_ALPHA = 0.85;

    enum Algorithm {
        STAMP,
        STOMP,
        TD_JOIN
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        Dotenv dotenv = Dotenv.load();
        String usr = dotenv.get("USERNAME");
        String pwd = dotenv.get("PASSWORD");
        String url = "bolt://localhost:7687";

        try (Driver driver = GraphDatabase.driver(url, AuthTokens.basic(usr, pwd))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");

            String delimiter = ";";
            boolean append = true;

            BufferedWriter writer = new BufferedWriter(
                    new FileWriter("src/main/resources/baseline_times.csv", append)
            );

            if (!append) {
                writer.write("dataset;algorithm;window;relation;ts;query_time\n");
            }

            String[] relations = {"overlaps"};

            /* ===================== FINBENCH ===================== */
            String[] finbenchDatasets = {
                    "finbench_50k",
                    "finbench_160k",
                    "finbench_500k",
                    "finbench_1m"
            };

            int[] finbenchWindows = {30};
            String[] finbenchTimeSeries = {"balance"};

            for (String dataset : finbenchDatasets) {


                loadDataset(dataset);
                System.out.println("Dataset loaded: " + dataset);

                int limit = switch (dataset) {
                    case "finbench_50k" -> 10000;
                    case "finbench_160k" -> 20000;
                    case "finbench_500k" -> 200000;
                    default -> 400000;
                };

                for (Algorithm algo: Algorithm.values()) {

                    System.out.println("Running " + algo + " on " + dataset);

                    String queryTemplate = buildFinbenchQuery(algo);

                    executeParametric(
                            driver,
                            writer,
                            delimiter,
                            algo,
                            dataset,
                            queryTemplate,
                            finbenchWindows,
                            finbenchTimeSeries,
                            relations,
                            limit
                    );
                }
            }

            /* ===================== SYNTHEA ===================== */
            String[] syntheaDatasets = {
                    "synthea_5k",
                    "synthea_10k",
                    "synthea_30k",
                    "synthea_100k"
            };

            int[] syntheaWindows = {5};
            String[] syntheaTimeSeries = {"bmi"};

            for (String dataset : syntheaDatasets) {

                if (dataset.equals("synthea_100k")) {
                    // skip if already loaded
                    System.out.println("Skipping dataset loading for " + dataset);
                }
                else
                    loadDataset(dataset);


                System.out.println("Dataset ready: " + dataset);

                for (Algorithm algo : Algorithm.values()) {

                    String syntheaQueryTemplate = buildSyntheaQuery(algo);

                    System.out.println("Running " + algo + " on " + dataset);

                    executeParametric(
                            driver,
                            writer,
                            delimiter,
                            algo,
                            dataset,
                            syntheaQueryTemplate,
                            syntheaWindows,
                            syntheaTimeSeries,
                            relations,
                            0   // no limit for synthea
                    );
                }
            }

            writer.close();
            System.out.println("STAMP + STOMP + TD_JOIN baseline completed.");
        }
    }

    /* ===================== QUERY BUILDERS ===================== */

    private static String buildFinbenchQuery(Algorithm algo) {

        return switch (algo) {

            case STAMP -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
                      (p1:Person)-[:Own]->(a1),
                      (a2)<-[:Own]-(p2:Person)
                WHERE p1.country <> p2.country
                LIMIT %d
                CALL edge2Time.StampTimeWalkPath(
                    p,
                    "%s",
                    "%s",
                    %d
                ) YIELD path
                RETURN count(*)
                """;

            case STOMP -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
                      (p1:Person)-[:Own]->(a1),
                      (a2)<-[:Own]-(p2:Person)
                WHERE p1.country <> p2.country
                LIMIT %d
                CALL edge2Time.StompTimeWalkPath(
                    p,
                    "%s",
                    "%s",
                    %d
                ) YIELD path
                RETURN count(*)
                """;

            case TD_JOIN -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
                      (p1:Person)-[:Own]->(a1),
                      (a2)<-[:Own]-(p2:Person)
                WHERE p1.country <> p2.country
                LIMIT %d
                CALL edge2Time.TimeWalkPathOpt(
                    p,
                    "%s",
                    "%s",
                    %d,
                    %f
                ) YIELD path
                RETURN count(*)
                """;
        };
    }

    private static String buildSyntheaQuery(Algorithm algo) {

        return switch (algo) {

            case STAMP -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient)
                CALL edge2Time.StampTimeWalkPath(
                    p,
                    "%s",
                    "%s",
                    %d
                ) YIELD path
                RETURN count(*)
                """;

            case STOMP -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient)
                CALL edge2Time.StompTimeWalkPath(
                    p,
                    "%s",
                    "%s",
                    %d
                ) YIELD path
                RETURN count(*)
                """;

            case TD_JOIN -> """
                CYPHER RUNTIME=PARALLEL
                MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient)
                CALL edge2Time.TimeWalkPathOpt(
                    p,
                    "%s",
                    "%s",
                    %d,
                    %f
                ) YIELD path
                RETURN count(*)
                """;
        };
    }

    /* ===================== CORE LOOP ===================== */

    private static void executeParametric(
            Driver driver,
            BufferedWriter writer,
            String delimiter,
            Algorithm algorithm,
            String dataset,
            String queryTemplate,
            int[] windows,
            String[] timeSeries,
            String[] relations,
            int limit
    ) throws IOException {

        boolean isSynthea = dataset.startsWith("synthea");

        for (String rel : relations) {
            for (String ts : timeSeries) {
                for (int w : windows) {

                    long totalTime = 0;

                    for (int i = 0; i < ITERATION; i++) {

                        String query;

                        if (algorithm == Algorithm.TD_JOIN) {
                            if (isSynthea) {
                                query = String.format(queryTemplate, ts, rel, w, TD_JOIN_ALPHA);
                            } else {
                                query = String.format(queryTemplate, limit, ts, rel, w, TD_JOIN_ALPHA);
                            }
                        } else {
                            if (isSynthea) {
                                query = String.format(queryTemplate, ts, rel, w);
                            } else {
                                query = String.format(queryTemplate, limit, ts, rel, w);
                            }
                        }

                        EagerResult res = driver.executableQuery(query).execute();
                        totalTime += res.summary()
                                .resultConsumedAfter(TimeUnit.MILLISECONDS);
                    }

                    long avgTime = totalTime / ITERATION;

                    writer.write(
                            dataset + delimiter +
                                    algorithm + delimiter +
                                    w + delimiter +
                                    rel + delimiter +
                                    ts + delimiter +
                                    avgTime + "\n"
                    );
                    writer.flush();
                }
            }
        }
    }


    /* ===================== DATASET LOADER ===================== */

    private static void loadDataset(String dataset)
            throws IOException, InterruptedException {

        ProcessBuilder pb = new ProcessBuilder(
                "sh", "src/main/resources/" + dataset + ".sh"
        );

        pb.redirectErrorStream(true);
        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException(
                    "Dataset loading failed for " + dataset
            );
        }
    }
}
