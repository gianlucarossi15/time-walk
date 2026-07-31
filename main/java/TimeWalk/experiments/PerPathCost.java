package TimeWalk.experiments;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * E1 — Per-path cost of path semantics.
 *
 * Goal: empirically validate Table III by isolating the per-hop cost difference
 * between PAIRWISE-CONTINUOUS (2cont) and CONTINUOUS (cont) across increasing
 * structural path length n. For each (relation, ts, w, iteration), we search
 * for a single candidate anchor (identified by a SKIP value) for which EVERY
 * path length n in pathLengths (evaluated in increasing order, starting at
 * n=1) yields a valid, all-nonzero-hop temporal path under BOTH semantics.
 *
 * The skip search is OUTER to the n loop: for a given skip value, we try
 * n=1, then n=2, ..., up to the largest n. If any n fails (no valid common
 * path for that skip), the ENTIRE chain is abandoned and we move to the next
 * skip value, restarting evaluation from n=1 — we never resume partway
 * through a failed chain. Only once a skip value succeeds for ALL n values
 * do we write results (one row per semantics per n) to the CSV.
 *
 * This search-then-write process is repeated for ITERATION iterations per
 * (relation, ts, w) — but only as long as each iteration actually finds a
 * valid chain. If an iteration's skip search exhausts MAX_SKIP_ATTEMPTS
 * without finding a chain valid for all n, the remaining iterations for that
 * (relation, ts, w) are skipped rather than retried, since the candidate
 * search space does not change between iterations and retrying would just
 * repeat the same failed search for nothing.
 *
 * We measure the average TD-Join cost (i.e. the time spent inside
 * TD_Join/TD_JoinPinned across all hops of the path, as reported by the UDF
 * itself, per-hop), NOT the total Cypher query execution time. The cache is
 * cleared between every query execution to avoid cross-call/cross-semantics
 * cache reuse confounding the measurement.
 */
public class PerPathCost {

    private static final double T = 0.85;
    private static final String clearCacheQuery = "CALL edge2Time.clearCache()";
    private static final String clearCache =" CALL db.clearQueryCaches()";
    private static int MAX_SKIP_ATTEMPTS = 1000;

    private record DatasetConfig(String name, String scriptPath, String pathQueryTemplate,
                                 String[] timeSeries, int[] windows) {}

    // Each query template has TWO placeholders, in order:
    //   1) %d  - path length n
    //   2) %d  - skip (candidate structural path index to try)
    // and parameterized $ts, $r, $w, $t. Both semantics are called on the
    // SAME candidate path p; the cache is cleared between the two calls so
    // TimeWalkPathCont's cacheable base-case hop never silently reuses
    // TimeWalkPath2Cont's already-warmed cache entry. path2Cont/pathCont and
    // their per-hop time lists (hops2Cont/hopsCont) are returned only when
    // BOTH are non-null and every hop's TD-Join time is non-zero.

    private static final String FINBENCH_PATH = """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer*%d]->(a2:Account)
        WHERE ALL(n IN nodes(p) WHERE n.balance_values IS NOT NULL)
        WITH p SKIP %d LIMIT 1
        CALL (p) {
          CALL edge2Time.TimeWalkPath2Cont(p, $ts, $r, $w, $t) YIELD path AS path2Cont, tdJoinTimeNs AS hops2Cont
          RETURN path2Cont, hops2Cont
        }
        CALL () {
          CALL edge2Time.clearCache() YIELD message
          RETURN message
        }
        CALL (p) {
          CALL edge2Time.TimeWalkPathCont(p, $ts, $r, $w, $t) YIELD path AS pathCont, tdJoinTimeNs AS hopsCont
          RETURN pathCont, hopsCont
        }
        WITH path2Cont, hops2Cont, pathCont, hopsCont
        WHERE path2Cont IS NOT NULL
          AND pathCont IS NOT NULL
          AND ALL(x IN hops2Cont WHERE x > 0)
          AND ALL(x IN hopsCont  WHERE x > 0)
        RETURN hops2Cont, hopsCont
        """;

    private static final String SYNTHEA_PATH = """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a:Patient)-[:WAS_CLASSMATE_OF*%d]->(b:Patient)
        WITH p SKIP %d LIMIT 1
        CALL (p) {
          CALL edge2Time.TimeWalkPath2Cont(p, $ts, $r, $w, $t) YIELD path AS path2Cont, tdJoinTimeNs AS hops2Cont
          RETURN path2Cont, hops2Cont
        }
        CALL () {
          CALL edge2Time.clearCache() YIELD message
          RETURN message
        }
        CALL (p) {
          CALL edge2Time.TimeWalkPathCont(p, $ts, $r, $w, $t) YIELD path AS pathCont, tdJoinTimeNs AS hopsCont
          RETURN pathCont, hopsCont
        }
        WITH path2Cont, hops2Cont, pathCont, hopsCont
        WHERE path2Cont IS NOT NULL
          AND pathCont IS NOT NULL
          AND ALL(x IN hops2Cont WHERE x > 0)
          AND ALL(x IN hopsCont  WHERE x > 0)
        RETURN hops2Cont, hopsCont
        """;

    private static final String NYC_PATH = """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE*%d]->(s2:Station)
        WITH p SKIP %d LIMIT 1
        CALL (p) {
          CALL edge2Time.TimeWalkPath2Cont(p, $ts, $r, $w, $t) YIELD path AS path2Cont, tdJoinTimeNs AS hops2Cont
          RETURN path2Cont, hops2Cont
        }
        CALL () {
          CALL edge2Time.clearCache() YIELD message
          RETURN message
        }
        CALL (p) {
          CALL edge2Time.TimeWalkPathCont(p, $ts, $r, $w, $t) YIELD path AS pathCont, tdJoinTimeNs AS hopsCont
          RETURN pathCont, hopsCont
        }
        WITH path2Cont, hops2Cont, pathCont, hopsCont
        WHERE path2Cont IS NOT NULL
          AND pathCont IS NOT NULL
          AND ALL(x IN hops2Cont WHERE x > 0)
          AND ALL(x IN hopsCont  WHERE x > 0)
        RETURN hops2Cont, hopsCont
        """;

    private static final String LA_PATH = """
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO*%d]->(s2:TRAFFIC_LIGHT)
        WITH p SKIP %d LIMIT 1
        CALL (p) {
          CALL edge2Time.TimeWalkPath2Cont(p, $ts, $r, $w, $t) YIELD path AS path2Cont, tdJoinTimeNs AS hops2Cont
          RETURN path2Cont, hops2Cont
        }
        CALL () {
          CALL edge2Time.clearCache() YIELD message
          RETURN message
        }
        CALL (p) {
          CALL edge2Time.TimeWalkPathCont(p, $ts, $r, $w, $t) YIELD path AS pathCont, tdJoinTimeNs AS hopsCont
          RETURN pathCont, hopsCont
        }
        WITH path2Cont, hops2Cont, pathCont, hopsCont
        WHERE path2Cont IS NOT NULL
          AND pathCont IS NOT NULL
          AND ALL(x IN hops2Cont WHERE x > 0)
          AND ALL(x IN hopsCont  WHERE x > 0)
        RETURN hops2Cont, hopsCont
        """;

    private static final String BAY_PATH = """
        MATCH p=(s1:TRAFFIC_LIGHT)-[:CONNECTED_TO*%d]->(s2:TRAFFIC_LIGHT)
        WITH p SKIP %d LIMIT 1
        CALL (p) {
          CALL edge2Time.TimeWalkPath2Cont(p, $ts, $r, $w, $t) YIELD path AS path2Cont, tdJoinTimeNs AS hops2Cont
          RETURN path2Cont, hops2Cont
        }
        CALL () {
          CALL edge2Time.clearCache() YIELD message
          RETURN message
        }
        CALL (p) {
          CALL edge2Time.TimeWalkPathCont(p, $ts, $r, $w, $t) YIELD path AS pathCont, tdJoinTimeNs AS hopsCont
          RETURN pathCont, hopsCont
        }
        WITH path2Cont, hops2Cont, pathCont, hopsCont
        WHERE path2Cont IS NOT NULL
          AND pathCont IS NOT NULL
          AND ALL(x IN hops2Cont WHERE x > 0)
          AND ALL(x IN hopsCont  WHERE x > 0)
        RETURN hops2Cont, hopsCont
        """;

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
                    new FileWriter("src/main/resources/per_path_cost.csv", append)
            );
            if (!append) {
                writer.write("dataset" + delimiter + "n" + delimiter + "semantics" + delimiter
                        + "window" + delimiter + "relation" + delimiter + "ts" + delimiter
                        + "iteration" + delimiter + "skip_used" + delimiter + "td_join_time_ms\n");
            }

            String[] relations  = {"before","equal","meets","overlaps"};
            int  pathLengths = 3;

            int[]    syntheaWindows      = {4, 5, 7};
            int[]    defaultWindows      = {30, 50, 70};
            String[] syntheaTimeSeries   = {"bmi", "bodyWeight", "bodyHeight", "heartRate", "respiratoryRate"};
            String[] finbenchTimeSeries  = {"balance"};
            String[] nycTimeSeries       = {"num_bikes_available", "num_docks_disabled",
                    "num_bikes_disabled", "num_ebikes_available"};
            String[] hourlySpeedTimeSeries = {"hourly_speed"};

            List<DatasetConfig> datasetConfigs = List.of(
                    new DatasetConfig("synthea",  "src/main/resources/synthea_30k.sh",  SYNTHEA_PATH,  syntheaTimeSeries,       syntheaWindows),
                    new DatasetConfig("bay",      "src/main/resources/bay.sh",          BAY_PATH,      hourlySpeedTimeSeries,   defaultWindows),
                    new DatasetConfig("finbench", "src/main/resources/finbench_500k.sh", FINBENCH_PATH, finbenchTimeSeries,      defaultWindows),
                    new DatasetConfig("nyc",      "src/main/resources/nyc.sh",          NYC_PATH,      nycTimeSeries,           defaultWindows),
                    new DatasetConfig("la",       "src/main/resources/la.sh",           LA_PATH,       hourlySpeedTimeSeries,   defaultWindows)
            );

            for (DatasetConfig config : datasetConfigs) {
                runDataset(config, relations, pathLengths,
                        driver, ITERATION, writer, delimiter);
            }

            writer.close();
            System.out.println("\nAll per-path cost experiments completed.");
        }
    }

    private static void runDataset(DatasetConfig config, String[] relations,
                                   int pathLengths, Driver driver, int ITERATION,
                                   BufferedWriter writer, String delimiter)
            throws IOException, InterruptedException {

        System.out.println("clearing cache...");
        driver.executableQuery(clearCache);

        for (String relation : relations) {
            for (String ts : config.timeSeries()) {
                for (int w : config.windows()) {

                    Map<Integer, List<Double>> td2ContByN = new LinkedHashMap<>();
                    Map<Integer, List<Double>> tdContByN  = new LinkedHashMap<>();
                    for (int n = 1; n <= pathLengths; n++) {
                        td2ContByN.put(n, new java.util.ArrayList<>());
                        tdContByN.put(n, new java.util.ArrayList<>());
                    }

                    // Once a skip value is found valid for all n, it's cached here
                    // and reused as the FIRST thing tried on the next iteration,
                    // instead of always rescanning from skip=0. The underlying
                    // data doesn't change between iterations, so a previously
                    // good skip will keep working — no need to pay for the full
                    // scan again.
                    int knownGoodSkip = -1;

                    for (int i = 0; i < ITERATION; i++) {

                        driver.executableQuery(clearCacheQuery).execute();

                        Map<Integer, Record> foundPerN = new LinkedHashMap<>();
                        int skipUsed = -1;
                        boolean allFound = false;

                        // Try the cached skip first, if we have one.
                        if (knownGoodSkip != -1) {
                            foundPerN = tryChainAtSkip(driver, config, ts, relation, w,
                                    pathLengths, knownGoodSkip, i, "cached");
                            if (foundPerN != null) {
                                skipUsed = knownGoodSkip;
                                allFound = true;
                            }
                        }

                        // Fall back to a full scan from skip=0 only if we don't
                        // have a cached skip, or the cached one stopped working.
                        if (!allFound) {
                            skipSearch:
                            for (int skip = 0; skip < MAX_SKIP_ATTEMPTS; skip++) {
                                if (skip == knownGoodSkip) {
                                    continue; // already tried above, don't repeat
                                }

                                Map<Integer, Record> attempt = new LinkedHashMap<>();
                                for (int n = 1; n <= pathLengths; n++) {
                                    String query = String.format(config.pathQueryTemplate(), n, skip);

                                    EagerResult res = driver.executableQuery(query)
                                            .withParameters(Map.of(
                                                    "ts", ts,
                                                    "r",  relation,
                                                    "w",  w,
                                                    "t",  T
                                            ))
                                            .execute();

                                    driver.executableQuery(clearCacheQuery).execute();

                                    List<Record> records = res.records();
                                    if (records.isEmpty()) {
                                        System.out.printf(
                                                "      rel=%s ts=%s w=%d iter=%d skip=%d -> chain broke at n=%d, retrying next skip from n=1%n",
                                                relation, ts, w, i, skip, n);
                                        continue skipSearch;
                                    }

                                    attempt.put(n, records.get(0));
                                }

                                foundPerN = attempt;
                                skipUsed = skip;
                                allFound = true;
                                break;
                            }
                        }

                        if (!allFound) {
                            System.out.printf(
                                    "      rel=%s ts=%s w=%d iter=%d -> DISCARDED (no skip found a valid chain for all n=1..%d within %d attempts), skipping remaining iterations for this combo%n",
                                    relation, ts, w, i, pathLengths, MAX_SKIP_ATTEMPTS);
                            break;
                        }

                        knownGoodSkip = skipUsed;

                        for (int n = 1; n <= pathLengths; n++) {
                            Record found = foundPerN.get(n);

                            List<Long> hops2Cont = found.get("hops2Cont").asList(Value::asLong);
                            List<Long> hopsCont  = found.get("hopsCont").asList(Value::asLong);

                            double td2ContMs = hops2Cont.stream().mapToLong(Long::longValue).sum() / 1_000_000.0;
                            double tdContMs  = hopsCont.stream().mapToLong(Long::longValue).sum()  / 1_000_000.0;

                            td2ContByN.get(n).add(td2ContMs);
                            tdContByN.get(n).add(tdContMs);

                            System.out.printf(
                                    "      n=%d rel=%s ts=%s w=%d iter=%d skip=%d td_2cont=%.4fms td_cont=%.4fms%n",
                                    n, relation, ts, w, i, skipUsed, td2ContMs, tdContMs);
                        }
                    }

                    for (int n = 1; n <= pathLengths; n++) {
                        List<Double> td2ContTimes = td2ContByN.get(n);
                        List<Double> tdContTimes  = tdContByN.get(n);
                        int successfulIterations = td2ContTimes.size();

                        if (successfulIterations == 0) {
                            continue;
                        }

                        double avgTd2Cont = td2ContTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                        double avgTdCont  = tdContTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                        writer.write(config.name() + delimiter + n + delimiter + "2cont" + delimiter
                                + w + delimiter + relation + delimiter + ts + delimiter
                                + avgTd2Cont + "\n");
                        writer.write(config.name() + delimiter + n + delimiter + "cont" + delimiter
                                + w + delimiter + relation + delimiter + ts + delimiter
                                + avgTdCont + "\n");
                        writer.flush();

                        System.out.printf(
                                "      AVG n=%d rel=%s ts=%s w=%d over %d iteration(s) td_2cont=%.4fms td_cont=%.4fms%n",
                                n, relation, ts, w, successfulIterations, avgTd2Cont, avgTdCont);
                    }
                }
            }
        }
    }

    /**
     * Tries a single candidate skip value against all n in 1..pathLengths.
     * Returns the per-n Record map if every n succeeds, or null if any n fails
     * (chain broken at that skip).
     */
    private static Map<Integer, Record> tryChainAtSkip(Driver driver, DatasetConfig config,
                                                       String ts, String relation, int w,
                                                       int pathLengths, int skip, int iter, String label) {
        Map<Integer, Record> foundPerN = new LinkedHashMap<>();

        for (int n = 1; n <= pathLengths; n++) {
            String query = String.format(config.pathQueryTemplate(), n, skip);

            EagerResult res = driver.executableQuery(query)
                    .withParameters(Map.of(
                            "ts", ts,
                            "r",  relation,
                            "w",  w,
                            "t",  T
                    ))
                    .execute();

            driver.executableQuery(clearCacheQuery).execute();

            List<Record> records = res.records();
            if (records.isEmpty()) {
                System.out.printf(
                        "      rel=%s ts=%s w=%d iter=%d skip=%d (%s) -> chain broke at n=%d, falling back to full scan%n",
                        relation, ts, w, iter, skip, label, n);
                return null;
            }

            foundPerN.put(n, records.get(0));
        }

        return foundPerN;
    }
}