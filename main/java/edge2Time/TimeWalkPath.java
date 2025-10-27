package edge2Time;

import apoc.create.Create;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static edge2Time.TDUtil.getTimeSeries;


public class TimeWalkPath {


    private static int countPath;

    public static class Result {
        public List<Relationship> path;

        public Result(List<Relationship> path) {
            this.path = path;
        }
    }


    private static List<Node> nodes = new ArrayList<>();

    public static int counter = 0;
    @Context
    public Log log;
    @Context
    public Transaction tx;

    private static DatabaseManagementService managementService;
    private static GraphDatabaseService db;

    @Procedure(name = "edge2Time.TimeWalkPath", mode = Mode.READ)
    public Stream<Result> testTransaction(@Name("Path") Path p,
                                          @Name("timeSeriesType") String tsType,
                                          @Name("r") String r,
                                          @Name("subsequenceLength") long w,
                                          @Name("threshold") double threshold) throws URISyntaxException, IOException {
        if (!Set.of("before", "meets", "equal", "overlaps","none").contains(r.toLowerCase())) {
            throw new IllegalArgumentException(r + " is not a valid Allen's relation. Please choose from 'before', 'meets', 'equal', or 'overlaps'.");
        }

        List<List<Relationship>> paths = new ArrayList<>();
        if (p.length() < 1 || p.length() < 1) {
            throw new IllegalArgumentException("Invalid depth parameters. minDepth and maxDepth must be >= 1 and minDepth <= maxDepth.");
        }
        countPath++;
        getThPaths(p, tsType, r, w, paths, threshold);
        if (paths.isEmpty())
            return Stream.empty();



        return paths.stream().map(Result::new);
    }

    Relationship getSingleHopPaths(Node source, Node target, String tsType, String r, long window, double threshold) {


        Relationship thEdge = null;
        List<DataPoint> ts1 = getTimeSeries(source, tsType);
        List<DataPoint> ts2 = getTimeSeries(target, tsType);
//        log.info("ts1 size: " + ts1.size() + ", ts2 size: " + ts2.size());
        if (ts1.isEmpty() || ts2.isEmpty() || ts1.size() < window || ts2.size() < window)
            return null;

        Instant startSource = ts1.get(0).getTimestamp();
        Instant endSource = ts1.get(ts1.size() - 1).getTimestamp();
        Instant startTarget = ts2.get(0).getTimestamp();
        Instant endTarget = ts2.get(ts2.size() - 1).getTimestamp();


//        boolean res = switch (r.toLowerCase()) {
//            case "equal" -> startSource.equals(startTarget);
//            case "meets" -> startTarget.plusSeconds(1).equals(endSource);
//            case "before" -> endSource.isBefore(startTarget) && !endSource.plusSeconds(1).equals(startTarget);
//            case "overlaps" ->
//                    startSource.isBefore(startTarget) && endSource.isAfter(startTarget) && endSource.isBefore(endTarget);
//            default -> false;
//        };
//        if (!res)
//            return thEdge;

        TS_JOIN t = new TS_JOIN();

        int sourcePairs = ts1.size() - (int) window + 1;
        int targetPairs = ts2.size() - (int) window + 1;
        int totalPairs = sourcePairs * targetPairs;
        long startTime = System.currentTimeMillis();
        Map<String, List<List<Double>>> ap = t.TS_Join(ts1, ts2, window, r, threshold);
        long endTime = System.currentTimeMillis();
        log.info("TD-Join cost " + (endTime - startTime) + " ms");


//        log.info("TS-Join execution time (ms): " + (endTime - startTime));
        if (!ap.get(r).isEmpty()) {
//            log.info("TS-Join produced results");

            int seqAIndex = TDUtil.findIndexes(ap, r)[0];
            int seqBIndex = TDUtil.findIndexes(ap, r)[1];

            List<DataPoint> sub1 = ts1.subList(seqAIndex, (int) (seqAIndex + window));
            List<DataPoint> sub2 = ts2.subList(seqBIndex, (int) (seqBIndex + window));
            Create creator = new Create();
            thEdge = creator.vRelationshipFunction(
                    source,
                    r,
                    Map.of("sourceSub", sub1.toString(),
                            "targetSub", sub2.toString(),
                            "tsType", tsType,
                            "subLength", window),
                    target
            );
            return thEdge;

        }
        return null;

    }

    public void getThPaths(Path p, String tsType, String r, long window, List<List<Relationship>> paths, double threshold) {
        List<Relationship> timeWalkPath = new ArrayList<>();
        Iterator<Node> nodeIter = p.nodes().iterator();
        if (!nodeIter.hasNext()) return;
        Node prev = nodeIter.next();
        while (nodeIter.hasNext()) {
            Node curr = nodeIter.next();
            Relationship rel = getSingleHopPaths(prev, curr, tsType, r, window, threshold);
            if (rel == null) {
                return; // Skip this path if any hop fails
            }
            counter++;
            timeWalkPath.add(rel);
            prev = curr;
        }
        if (!timeWalkPath.isEmpty()) {
            paths.add(timeWalkPath);
        }
    }

}



