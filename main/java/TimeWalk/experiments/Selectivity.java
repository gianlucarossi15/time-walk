package TimeWalk.experiments;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Selectivity {

    private static final double T = 0;

    private static final String clearCacheQuery = "CALL edge2Time.clearCache()";

    // requiresTsFilter: true when the dataset's queries contain an extra "%s"
    // placeholder (before the procedure-name "%s") for
    // "ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)". This is needed because a
    // time-series property name can't be bound as a Cypher parameter — it has
    // to be substituted into the query text, same as the procedure name.
    private record DatasetConfig(String name, String scriptPath, String[][] queries, String[] timeSeries,
                                 int[] windows, boolean requiresTsFilter) {}

    // queryPairs[i][0] = selectivity query.
    //   - requiresTsFilter == false: no placeholders, run as-is.
    //   - requiresTsFilter == true:  one "%s" placeholder for the ts property name.
    // queryPairs[i][1] = timed query.
    //   - requiresTsFilter == false: one "%s" placeholder for the procedure name.
    //   - requiresTsFilter == true:  two "%s" placeholders, in this order:
    //        1) ts property name (inside the WHERE clause)
    //        2) procedure name   (inside the CALL clause)
    //     "%s" placeholders are substituted positionally in the order they
    //     appear in the string, so this order must match String.format's args.

    public static final String [][] FINBENCH_MIRRORING = {
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        WHERE p1.country <> p2.country
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        WHERE p1.country <> p2.country
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer|Withdraw]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer|Withdraw]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN COUNT(*)
        """
            }
    };

    public static final String [][] SYNTHEA_MIRRORING = {
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient) CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:MOTHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:MOTHER_OF]->(b:Patient) CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:WAS_CLASSMATE_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:WAS_CLASSMATE_OF]->(b:Patient) CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF|MOTHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF|MOTHER_OF]->(b:Patient) CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path RETURN count(*)
        """
            }
    };

    // NYC queries now filter for the presence of the time-series property on
    // every node in the path, via "ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)".
    // Without this, sel/timed queries could count stations that never had
    // the given time series set at all, skewing selectivity.
    public static final String [][] NYC_MIRRORING = {
            {
                    """ 
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE s1.region_id <> s2.region_id AND ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE s1.region_id <> s2.region_id AND ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE s1.region_id <> s2.region_id AND ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE s1.region_id <> s2.region_id AND ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE ALL(n IN nodes(p) WHERE n.%s_values IS NOT NULL)
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path
        RETURN count(*)
        """
            }
    };

    public static final String [][] LA_MIRRORING = {
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
        AND ABS(s1.longitude - s2.longitude) < 0.002
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
          AND ABS(s1.longitude - s2.longitude) < 0.002
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
       """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
        AND ABS(s1.longitude - s2.longitude) < 0.002
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
          AND ABS(s1.longitude - s2.longitude) < 0.002
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.001
        AND ABS(s1.longitude - s2.longitude) < 0.001
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.001
          AND ABS(s1.longitude - s2.longitude) < 0.001
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
       """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.001
        AND ABS(s1.longitude - s2.longitude) < 0.001
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.001
          AND ABS(s1.longitude - s2.longitude) < 0.001
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
        """
            }
    };

    public static final String [][] BAY_MIRRORING = {
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
        AND ABS(s1.longitude - s2.longitude) < 0.002
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
        where  ABS(s1.latitude - s2.latitude) < 0.002
          AND ABS(s1.longitude - s2.longitude) < 0.002
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
       """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.0005
                AND ABS(s1.longitude - s2.longitude) < 0.0005
                RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]->(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.0005
                AND ABS(s1.longitude - s2.longitude) < 0.0005
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
        """
            },
            {
                    """
         CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.002
                AND ABS(s1.longitude - s2.longitude) < 0.002
                limit 20
                RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.002
                AND ABS(s1.longitude - s2.longitude) < 0.002
                limit 20
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
       """
            },
            {
                    """
         CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.002
                AND ABS(s1.longitude - s2.longitude) < 0.002
                limit 10
                RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
                MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO]-(s2:TRAFFIC_LIGHT)
                where  ABS(s1.latitude - s2.latitude) < 0.002
                AND ABS(s1.longitude - s2.longitude) < 0.002
                limit 10
        CALL edge2Time.%s(p, $ts, $r, $w, $t) YIELD path return count(*)
       """
            }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        var url = "bolt://localhost:7687";
        final int ITERATION = 5;
        Dotenv dotenv = Dotenv.load();
        var usr = dotenv.get("USERNAME");
        var pwd = dotenv.get("PASSWORD");

        try (var driver = GraphDatabase.driver(url, AuthTokens.basic(usr, pwd))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");
            String delimiter = ";";

            boolean append = true;

            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(
                            String.format("src/main/resources/selectivity_time.csv"),
                            append
                    )
            );
            if (!append)
                writer.write("dataset" + delimiter + "queryId" + delimiter + "semantics" + delimiter
                        + "window" + delimiter + "relation" + delimiter + "ts" + delimiter
                        + "selectivity" + delimiter + "query_time\n");

            String[] relations = {"overlaps", "before", "meets", "equal"};
//            String [] relations = {"equal","meets"};
            String[] semantics = {"2cont"};

            int[] syntheaWindows = {4, 5, 7};
            int[] defaultWindows = {30, 50, 70};
            String[] syntheaTimeSeries = {"bmi", "bodyWeight", "bodyHeight", "heartRate", "respiratoryRate"};
            String[] finbenchTimeSeries = {"balance"};
            String[] nycTimeSeries = {"num_bikes_available", "num_docks_disabled", "num_bikes_disabled", "num_ebikes_available"};
            String[] hourlySpeedTimeSeries = {"hourly_speed"};

            List<DatasetConfig> datasetConfigs = List.of(
                    new DatasetConfig("synthea", "src/main/resources/synthea_30k.sh", SYNTHEA_MIRRORING, syntheaTimeSeries, syntheaWindows, false),
                    new DatasetConfig("bay", "src/main/resources/bay.sh", BAY_MIRRORING, hourlySpeedTimeSeries, defaultWindows, false),
                    new DatasetConfig("finbench", "src/main/resources/finbench_500k.sh", FINBENCH_MIRRORING, finbenchTimeSeries, defaultWindows, false),
                    new DatasetConfig("nyc", "src/main/resources/nyc.sh", NYC_MIRRORING, nycTimeSeries, defaultWindows, true),
                    new DatasetConfig("la", "src/main/resources/la.sh", LA_MIRRORING, hourlySpeedTimeSeries, defaultWindows, false)
            );

            for (DatasetConfig config : datasetConfigs) {
                runDataset(relations, semantics, config.timeSeries(), config.windows(), config.queries(),
                        driver, ITERATION, writer, config.name(), delimiter, config.scriptPath(), config.requiresTsFilter());
            }

            writer.close();
            System.out.println("\nAll selectivity experiments completed.");

        }
    }

    private static void runDataset(String[] relations, String[] semantics, String[] timeSeries, int[] windows,
                                   String[][] dataset, Driver driver, int ITERATION, BufferedWriter writer,
                                   String datasetName, String delimiter, String scriptPath,
                                   boolean requiresTsFilter) throws IOException, InterruptedException {
        int queryId = 1;

        if (scriptPath != null) {
            ProcessBuilder pb = new ProcessBuilder("sh", scriptPath);
            pb.inheritIO();
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException(datasetName + " script failed with exit code " + exitCode);
            }
        }

        System.out.printf("%nLoaded: %s%n", datasetName);

        for (String[] query : dataset) {
            for (String sem : semantics) {
                String procedureName = "TimeWalkPath" + (sem.equals("2cont") ? "2Cont" : "Cont");
                System.out.println("  Semantics: " + sem);

                // sel depends on the time-series property whenever the query
                // filters on "n.%s_values IS NOT NULL", so it must be recomputed per
                // ts rather than once per query pair. It does NOT depend on
                // relation or window, so it's computed once per ts and reused
                // across both of those loops.
                for (String ts : timeSeries) {
                    String selectivityQuery = requiresTsFilter
                            ? String.format(query[0], ts)
                            : query[0];

                    EagerResult selRes = driver.executableQuery(selectivityQuery).execute();
                    long value = selRes.records().get(0).get("sel").asLong();

                    for (String relation : relations) {
                        for (int w : windows) {
                            long totalTime = 0;
                            for (int i = 0; i < ITERATION; i++) {
                                String newQuery = requiresTsFilter
                                        ? String.format(query[1], ts, procedureName)
                                        : String.format(query[1], procedureName);
                                EagerResult res = driver.executableQuery(newQuery)
                                        .withParameters(Map.of(
                                                "ts", ts,
                                                "r", relation,
                                                "w", w,
                                                "t", T
                                        ))
                                        .execute();
                                totalTime += res.summary().resultConsumedAfter(TimeUnit.MILLISECONDS);
                                driver.executableQuery(clearCacheQuery).execute();
                            }
                            long avgTime = totalTime / ITERATION;
                            writer.write(datasetName + delimiter + queryId + delimiter + sem + delimiter
                                    + w + delimiter + relation + delimiter + ts + delimiter
                                    + value + delimiter + avgTime + "\n");
                            writer.flush();

                            System.out.printf("      Q%d — sel=%d, avg=%dms%n", queryId, value, avgTime);
                        }
                    }
                }
            }
            queryId++;
        }
    }
}