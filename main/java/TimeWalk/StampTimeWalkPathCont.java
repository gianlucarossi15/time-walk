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
 * Continuous TS-Path semantics (CONT) implemented via STAMP.
 *
 * Mirrors TimeWalkPathCont exactly, replacing TD_JOINOptimized with
 * StampWithAllen:
 *
 *   - Hop 1  : StampWithAllen.myStamp evaluated freely over (ts1, ts2).
 *              Result is cached (key = source->target|tsType|r|w|threshold).
 *
 *   - Hop i>=2: The source subsequence is PINNED to the previous hop's
 *              target subsequence I_{i-1}^{(2)}. myStampPinned then searches
 *              only over target candidates in ts2 that stand in relation R
 *              with the pinned source.
 *              NOT cached — the pinned source varies per path.
 *
 * If any hop returns null the entire path is invalid (strict, no backtracking).
 */
public class StampTimeWalkPathCont {

    private static int countPath;
    public static int counter   = 0;
    public static int cacheHits = 0;

    public static class Result {
        public List<Relationship> path;

        public Result(List<Relationship> path) {
            this.path = path;
        }
    }

    @Context
    public Log log;

    @Context
    public Transaction tx;

    // ------------------------------------------------------------------
    //  Procedure entry-point
    // ------------------------------------------------------------------

    @Procedure(name = "edge2Time.StampTimeWalkPathCont", mode = Mode.READ)
    public Stream<Result> stampTimeWalkPathCont(
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

        List<List<Relationship>> paths = new ArrayList<>();
        getContPaths(p, tsType, r, w, paths, threshold);

        System.out.println("cache hits: " + cacheHits);

        if (paths.isEmpty()) return Stream.empty();
        return paths.stream().map(Result::new);
    }

    // ------------------------------------------------------------------
    //  Path walk — continuous chaining (pinned source for hops i>=2)
    // ------------------------------------------------------------------

    public void getContPaths(Path p, String tsType, String r, long window,
                             List<List<Relationship>> paths, double threshold) {

        List<Relationship> timeWalkPath = new ArrayList<>();

        Iterator<Node> nodeIter = p.nodes().iterator();
        if (!nodeIter.hasNext()) return;

        Node prev = nodeIter.next();

        // null  → base case (hop 1, free, cacheable)
        // set   → recursive step (hop i>=2, pinned source, not cached)
        Integer pinnedSrcIndex = null;

        while (nodeIter.hasNext()) {

            Node curr = nodeIter.next();

            HopResult hop = getSingleHopPinned(prev, curr, tsType, r, window,
                    threshold, pinnedSrcIndex);

            // Strict CONT: one failed hop invalidates the whole path
            if (hop == null) return;

            counter++;
            timeWalkPath.add(hop.edge);

            // The target subsequence of this hop becomes the pinned
            // source of the next hop.
            pinnedSrcIndex = hop.tgtIndex;

            prev = curr;
        }

        if (!timeWalkPath.isEmpty()) {
            paths.add(timeWalkPath);
        }
    }

    // ------------------------------------------------------------------
    //  Single-hop evaluation
    //
    //  pinnedSrcIndex == null  →  free hop (hop 1): uses cache
    //  pinnedSrcIndex != null  →  pinned hop (hop i>=2): source is the
    //                             single subsequence ts1[pinnedSrcIndex :
    //                             pinnedSrcIndex + window]; only target
    //                             candidates in ts2 are explored.
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

                StampWithAllen st = new StampWithAllen();
                ap = st.myStamp(ts1, ts2, (int) window, r);

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

            StampWithAllen st = new StampWithAllen();
            ap = st.myStampPinned(pinnedSource, ts2, (int) window, r);

            boolean match = ap.get(r) != null && !ap.get(r).isEmpty();
            if (!match) return null;

            int[] idx = TDUtil.findIndexes(ap, r);

            // Source is pinned; idx[0] is unused (always 0 by construction).
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

        return new HopResult(edge, tgtIndex);
    }

    // ------------------------------------------------------------------
    //  Internal carrier
    // ------------------------------------------------------------------

    static class HopResult {
        final Relationship edge;
        /** Absolute start index of I_i^{(2)} in τ_{target}. */
        final int tgtIndex;

        HopResult(Relationship edge, int tgtIndex) {
            this.edge     = edge;
            this.tgtIndex = tgtIndex;
        }
    }

    // ------------------------------------------------------------------
    //  Cache key
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