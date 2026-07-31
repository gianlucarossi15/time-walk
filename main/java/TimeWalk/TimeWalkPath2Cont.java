package TimeWalk;

import apoc.create.Create;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;

import static TimeWalk.TDUtil.getTimeSeries;

public class TimeWalkPath2Cont {

    @Context
    public Log log;

    @Context
    public Transaction tx;

    public static int cacheHits = 0;

    // =====================================================
    // RESULT
    // =====================================================
    public static class Result {
        public List<Relationship> path;
        public List<Long> tdJoinTimeNs; // per-hop TD-Join compute time (ns) for this path

        public Result(List<Relationship> path, List<Long> tdJoinTimeNs) {
            this.path = path;
            this.tdJoinTimeNs = tdJoinTimeNs;
        }
    }

    // =====================================================
    // PROCEDURE
    // =====================================================
    @Procedure(name = "edge2Time.TimeWalkPath2Cont", mode = Mode.READ)
    public Stream<Result> testTransaction(@Name("Path") Path p,
                                          @Name("timeSeriesType") String tsType,
                                          @Name("r") String r,
                                          @Name("subsequenceLength") long w,
                                          @Name("threshold") double threshold)
            throws URISyntaxException, IOException {

        if (!Set.of("before", "meets", "equal", "overlaps", "none")
                .contains(r.toLowerCase())) {
            throw new IllegalArgumentException("Invalid relation");
        }

        if (p.length() < 1) {
            throw new IllegalArgumentException("Path length must be >= 1");
        }

        CacheManager.initDB();

        List<Result> results = new ArrayList<>();

        getThPaths(p, tsType, r, w, threshold, results);

        System.out.println("cache hits: " + cacheHits);

        return results.stream();
    }

    // =====================================================
    // SINGLE HOP
    // =====================================================
    HopEdge getSingleHopPaths(Node source, Node target,
                              String tsType, String r,
                              long window, double threshold) {

        List<DataPoint> ts1 = getTimeSeries(source, tsType);
        List<DataPoint> ts2 = getTimeSeries(target, tsType);

        if (ts1.isEmpty() || ts2.isEmpty()
                || ts1.size() < window || ts2.size() < window)
            return null;

        String key = source.getElementId() + "->" + target.getElementId()
                + "|" + tsType + "|" + r + "|" + window + "|" + threshold;

        var cache = CacheManager.getCache();
        CacheManager.CachedResult cached = cache.get(key);

        Map<String, List<double[]>> ap;
        long tdJoinTimeNs = 0; // stays 0 on a cache hit: no TD-Join computation performed

        if (cached != null) {

            if (!cached.match) return null;

            ap = cached.result;

            cacheHits++;

        } else {

            TD_JOINOptimized t = new TD_JOINOptimized();

            long start = System.nanoTime();
            ap = t.TD_Join(ts1, ts2, window, r, threshold);
            tdJoinTimeNs = System.nanoTime() - start;

            boolean match = ap.get(r) != null && !ap.get(r).isEmpty();

            if (!match) {
                cache.put(key, new CacheManager.CachedResult(ap, false, -1, -1));
                return null;
            }

            int[] idx = TDUtil.findIndexes(ap, r);

            cache.put(key,
                    new CacheManager.CachedResult(ap, true, idx[0], idx[1]));
        }

        int seqA = cached != null ? cached.seqAIndex : TDUtil.findIndexes(ap, r)[0];
        int seqB = cached != null ? cached.seqBIndex : TDUtil.findIndexes(ap, r)[1];

        List<DataPoint> sub1 = ts1.subList(seqA, (int) (seqA + window));
        List<DataPoint> sub2 = ts2.subList(seqB, (int) (seqB + window));

        Create creator = new Create();

        Relationship rel = creator.vRelationshipFunction(
                source,
                r,
                Map.of(
                        "sourceSub", sub1.toString(),
                        "targetSub", sub2.toString(),
                        "tsType", tsType,
                        "subLength", window,
                        "threshold", threshold
                ),
                target
        );

        return new HopEdge(rel, tdJoinTimeNs);
    }

    // =====================================================
    // PATH WALK
    // =====================================================
    public void getThPaths(Path p,
                           String tsType,
                           String r,
                           long window,
                           double threshold,
                           List<Result> results) {

        List<Relationship> timeWalkPath = new ArrayList<>();
        List<Long> hopTdJoinTimesNs = new ArrayList<>();

        Iterator<Node> it = p.nodes().iterator();
        if (!it.hasNext()) return;

        Node prev = it.next();

        while (it.hasNext()) {

            Node curr = it.next();

            HopEdge hop =
                    getSingleHopPaths(prev, curr, tsType, r, window, threshold);

            if (hop == null) return;

            timeWalkPath.add(hop.edge);
            hopTdJoinTimesNs.add(hop.tdJoinTimeNs);
            prev = curr;
        }

        if (!isValidTemporalPath(timeWalkPath, r)) {
            return;
        }

        results.add(new Result(timeWalkPath, hopTdJoinTimesNs));
    }

    private boolean isValidTemporalPath(List<Relationship> path, String r) {

        if (path == null || path.isEmpty()) {
            log.warn("Rejected temporal path: path is empty.");
            return false;
        }

        for (Relationship rel : path) {
            if (rel == null) {
                log.warn("Rejected temporal path: found a null relationship.");
                return false;
            }
            if (!rel.isType(RelationshipType.withName(r))) {
                log.warn("Rejected temporal path: relationship type {} does not match expected r={}.",
                        rel.getType().name(), r);
                return false;
            }
        }

        return true;
    }

    // =====================================================
    // Internal carrier
    // =====================================================
    static class HopEdge {
        final Relationship edge;
        /** Time (ns) spent inside TD_Join for this hop. 0 on cache hit. */
        final long tdJoinTimeNs;

        HopEdge(Relationship edge, long tdJoinTimeNs) {
            this.edge = edge;
            this.tdJoinTimeNs = tdJoinTimeNs;
        }
    }
}