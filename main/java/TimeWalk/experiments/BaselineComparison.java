package TimeWalk.experiments;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BaselineComparison {

    private static final int ITERATION = 5;
    private static final double T = 0.85;

    private static final String clearCacheQuery = "CALL edge2Time.clearCache()";

    // ------------------------------------------------------------------ records

    record SizedDataset(
            String scriptName,
            String displaySize,
            String[] mirroringQueries,
            String timeSeries,
            String relation,
            int window
    ) {}

    // ================================================================== SYNTHEA

    static final String[] SYNTHEA_MIRRORING = {
            """
            CYPHER RUNTIME=PARALLEL
            MATCH p=(a:Patient)-[:WAS_CLASSMATE_OF]->(b:Patient)
            CALL edge2Time.%s(p, $ts, $r, $w, $t)
            YIELD path RETURN count(path)
            """
    };

    // ================================================================= FINBENCH

    static final String[] FINBENCH_MIRRORING = {
            """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account{accountLevel:"Silver level"})-[:Transfer]->(a2:Account{accountLevel:"Platinum level"})
        WHERE ALL(n IN nodes(p) WHERE n.balance_values IS NOT NULL)
        CALL edge2Time.%s(p, $ts, $r, $w, $t)
        YIELD path RETURN count(path)
        """
    };

    // ================================================================= MAIN

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
                    new FileWriter("src/main/resources/baseline_time.csv", append)
            );

            if (!append) {
                writer.write(
                        "dataset" + delimiter +
                                "datasetSize" + delimiter +
                                "method" + delimiter +
                                "window" + delimiter +
                                "relation" + delimiter +
                                "ts" + delimiter +
                                "query_time\n"
                );
            }

            List<SizedDataset> sizedDatasets = List.of(

                    new SizedDataset("synthea_5k", "5k", SYNTHEA_MIRRORING, "bmi", "overlaps", 4),
                    new SizedDataset("synthea_10k", "10k", SYNTHEA_MIRRORING, "bmi", "overlaps", 4),
                    new SizedDataset("synthea_30k", "30k", SYNTHEA_MIRRORING, "bmi", "overlaps", 4),
                    new SizedDataset("synthea_100k", "100k", SYNTHEA_MIRRORING, "bmi", "overlaps", 4),

                    new SizedDataset("finbench_50k", "50k", FINBENCH_MIRRORING, "balance", "overlaps", 30),
                    new SizedDataset("finbench_160k", "160k", FINBENCH_MIRRORING, "balance", "overlaps", 30),
                    new SizedDataset("finbench_500k", "500k", FINBENCH_MIRRORING, "balance", "overlaps", 30),
                    new SizedDataset("finbench_1m", "1M", FINBENCH_MIRRORING, "balance", "overlaps", 30)
            );

            String[] methods = {"TD-Join","STAMP","STOMP"};
//            String[] methods ={"STOMP"};
            String[] allenRelations = {"overlaps", "meets", "equal", "before"};

            for (SizedDataset sd : sizedDatasets) {

                String datasetLabel = sd.scriptName().replaceAll("_\\d.*$", "");

                loadDataset(sd.scriptName());
                System.out.printf("%nLoaded: %s (%s)%n", datasetLabel, sd.displaySize());

                for (String relation : allenRelations) {

                    System.out.println("  Relation: " + relation);

                    for (String method : methods) {

                        String base = switch (method) {
                            case "TD-Join" -> "TimeWalkPath";
                            case "STAMP"   -> "StampTimeWalkPath";
                            case "STOMP"   -> "StompTimeWalkPath";
                            default -> throw new IllegalArgumentException();
                        };

                        String pluginName = base + "2Cont";
                        boolean hasThreshold = true;

                        System.out.println("    Method: " + method);

                        for (String query : sd.mirroringQueries()) {

                            long totalTime = 0;

                            for (int iter = 0; iter < ITERATION; iter++) {

                                String timedQuery = String.format(query, pluginName);

                                EagerResult res = driver
                                        .executableQuery(timedQuery)
                                        .withParameters(Map.of(
                                                "ts", sd.timeSeries(),
                                                "r", relation,
                                                "w", sd.window(),
                                                "t", hasThreshold ? T : 0.0
                                        ))
                                        .execute();

                                totalTime += res.summary().resultConsumedAfter(TimeUnit.MILLISECONDS);
                                driver.executableQuery(clearCacheQuery).execute();
                            }

                            long avgTime = totalTime / ITERATION;

                            writer.write(
                                    datasetLabel + delimiter +
                                            sd.displaySize() + delimiter +
                                            method + delimiter +
                                            sd.window() + delimiter +
                                            relation + delimiter +
                                            sd.timeSeries() + delimiter +
                                            avgTime + "\n"
                            );

                            writer.flush();

                            System.out.printf("      rel=%s, avg=%dms%n", relation, avgTime);
                        }
                    }
                }
            }

            writer.close();
            System.out.println("\nAll baselines completed.");
        }
    }

    private static void loadDataset(String scriptName) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("sh", "src/main/resources/" + scriptName + ".sh");
        pb.redirectErrorStream(true);
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Dataset loading failed for: " + scriptName);
        }
    }
}