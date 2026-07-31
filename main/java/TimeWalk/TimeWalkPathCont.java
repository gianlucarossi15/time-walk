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

/**
 * Continuous TS-Path semantics (CONT).
 * ... (unchanged doc)
 */
public class TimeWalkPathCont {

    private static int countPath;
    public static int counter   = 0;
    public static int cacheHits = 0;

    public static class Result {
        public List<Relationship> path;
        public List<Long> tdJoinTimeNs; // per-hop TD-Join compute time (ns) for this path

        public Result(List<Relationship> path, List<Long> tdJoinTimeNs) {
            this.path = path;
            this.tdJoinTimeNs = tdJoinTimeNs;
        }
    }

    @Context
    public Log log;

    @Context
    public Transaction tx;

    // ------------------------------------------------------------------
    //  Procedure entry-point
    // ------------------------------------------------------------------

    @Procedure(name = "edge2Time.TimeWalkPathCont", mode = Mode.READ)
    public Stream<Result> timeWalkPathCont(
            @Name("Path")              Path   p,
            @Name("timeSeriesType")    String tsType,
            @Name("r")                 String r,
            @Name("subsequenceLength") long   w,
            @Name("threshold")         double threshold)
            throws URISyntaxException, IOException {

        if (!Set.of("before", "meets", "equal", "overlaps", "none")
                .contains(r.toLowerCase())) {
            throw new IllegalArgumentException(
                    r + " is not a valid Allen's relation. " +
                            "Please choose from 'before', 'meets', 'equal', or 'overlaps'.");
        }

        if (p.length() < 1) {
            throw new IllegalArgumentException("Invalid path length.");
        }

        CacheManager.initDB();

        countPath++;

        List<Result> results = new ArrayList<>();
        getContPaths(p, tsType, r, w, results, threshold);

        System.out.println("cache hits: " + cacheHits);

        return results.stream();
    }

    // ------------------------------------------------------------------
    //  Path walk — continuous chaining (pinned source for hops i≥2)
    // ------------------------------------------------------------------

    public void getContPaths(Path p, String tsType, String r, long window,
                             List<Result> results, double threshold) {

        List<Relationship> timeWalkPath = new ArrayList<>();
        List<Long> hopTdJoinTimesNs = new ArrayList<>();

        Iterator<Node> nodeIter = p.nodes().iterator();
        if (!nodeIter.hasNext()) return;

        Node prev = nodeIter.next();

        // null  → base case (hop 1, free, cacheable)
        // set   → recursive step (hop i≥2, pinned source, not cached)
        Integer pinnedSrcIndex = null;

        while (nodeIter.hasNext()) {

            Node curr = nodeIter.next();

            HopResult hop = getSingleHopPinned(prev, curr, tsType, r, window,
                    threshold, pinnedSrcIndex);

            // Strict CONT: one failed hop invalidates the whole path
            if (hop == null) return;

            counter++;
            timeWalkPath.add(hop.edge);
            hopTdJoinTimesNs.add(hop.tdJoinTimeNs);

            // The target subsequence of this hop becomes the pinned
            // source of the next hop.
            pinnedSrcIndex = hop.tgtIndex;

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

    // ------------------------------------------------------------------
    //  Single-hop evaluation
    // ------------------------------------------------------------------

    HopResult getSingleHopPinned(Node source, Node target,
                                 String tsType, String r, long window,
                                 double threshold, Integer pinnedSrcIndex) {

        List<DataPoint> ts1 = getTimeSeries(source, tsType);
        List<DataPoint> ts2 = getTimeSeries(target, tsType);

        if (ts1.isEmpty() || ts2.isEmpty()
                || ts1.size() < window || ts2.size() < window)
            return null;

        Map<String, List<double[]>> ap;
        int srcIndex;
        int tgtIndex;
        long tdJoinTimeNs = 0; // stays 0 on a cache hit: no TD-Join computation performed

        if (pinnedSrcIndex == null) {

            // ---- BASE CASE: free, cacheable --------------------------------

            String key = cacheKey(source, target, tsType, r, window, threshold);
            var cache  = CacheManager.getCache();

            CacheManager.CachedResult cached = cache.get(key);

            if (cached != null) {

                if (!cached.match) return null;

                ap       = cached.result;
                srcIndex = cached.seqAIndex;
                tgtIndex = cached.seqBIndex;
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
                cache.put(key, new CacheManager.CachedResult(ap, true, idx[0], idx[1]));

                srcIndex = idx[0];
                tgtIndex = idx[1];
            }

        } else {

            // ---- RECURSIVE STEP: pinned source, not cached -----------------

            if (pinnedSrcIndex < 0
                    || pinnedSrcIndex + window > ts1.size()) return null;

            List<DataPoint> pinnedSource =
                    ts1.subList(pinnedSrcIndex, pinnedSrcIndex + (int) window);

            TD_JOINOptimized t = new TD_JOINOptimized();

            long start = System.nanoTime();
            ap = t.TD_JoinPinned(pinnedSource, ts2, window, r, threshold);
            tdJoinTimeNs = System.nanoTime() - start;

            boolean match = ap.get(r) != null && !ap.get(r).isEmpty();
            if (!match) return null;

            int[] idx = TDUtil.findIndexes(ap, r);

            srcIndex = pinnedSrcIndex;
            tgtIndex = idx[1];
        }

        List<DataPoint> sub1 = ts1.subList(srcIndex, (int) (srcIndex + window));
        List<DataPoint> sub2 = ts2.subList(tgtIndex, (int) (tgtIndex + window));

        Relationship edge = new Create().vRelationshipFunction(
                source,
                r,
                Map.of(
                        "sourceSub", sub1.toString(),
                        "targetSub", sub2.toString(),
                        "tsType",    tsType,
                        "subLength", window,
                        "threshold", threshold
                ),
                target
        );

        return new HopResult(edge, tgtIndex, tdJoinTimeNs);
    }

    // ------------------------------------------------------------------
    //  Internal carrier
    // ------------------------------------------------------------------

    static class HopResult {
        final Relationship edge;
        /** Absolute start index of I_i^{(2)} in τ_{target}. */
        final int tgtIndex;
        /** Time (ns) spent inside TD_Join/TD_JoinPinned for this hop. 0 on cache hit. */
        final long tdJoinTimeNs;

        HopResult(Relationship edge, int tgtIndex, long tdJoinTimeNs) {
            this.edge         = edge;
            this.tgtIndex     = tgtIndex;
            this.tdJoinTimeNs = tdJoinTimeNs;
        }
    }

    // ------------------------------------------------------------------
    //  Cache key — same format as CachedTimeWalkPathOptimized
    // ------------------------------------------------------------------

    private static String cacheKey(Node a, Node b,
                                   String tsType, String r,
                                   long window, double threshold) {
        return a.getElementId() + "->" + b.getElementId()
                + "|" + tsType
                + "|" + r
                + "|" + window
                + "|" + threshold;
    }
}