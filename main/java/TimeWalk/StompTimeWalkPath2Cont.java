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

public class StompTimeWalkPath2Cont {

    public static int counter = 0;

    @Context
    public Log log;

    @Context
    public Transaction tx;

    // =====================================================
    // RESULT (FIXED)
    // =====================================================
    public static class Result {
        public List<Relationship> path;

        public Result(List<Relationship> path) {
            this.path = path;
        }
    }

    // =====================================================
    // CACHE KEY
    // =====================================================
    private static String cacheKey(Node source, Node target,
                                   String tsType, String r,
                                   long window,
                                   double threshold) {

        return source.getElementId() + "->" + target.getElementId()
                + "|" + tsType
                + "|" + r
                + "|" + window
                + "|" + threshold;
    }

    // =====================================================
    // PROCEDURE
    // =====================================================
    @Procedure(name = "edge2Time.StompTimeWalkPath2Cont", mode = Mode.READ)
    public Stream<Result> stompTimeWalkPath2Cont(
            @Name("Path") Path p,
            @Name("timeSeriesType") String tsType,
            @Name("r") String r,
            @Name("subsequenceLength") long w,
            @Name("threshold") double threshold)
            throws URISyntaxException, IOException {

        if (!Set.of("before", "meets", "equal", "overlaps", "none")
                .contains(r.toLowerCase())) {
            throw new IllegalArgumentException(r + " is not valid.");
        }

        if (p.length() < 1) {
            throw new IllegalArgumentException("Path length must be >= 1.");
        }

        CacheManager.initDB();

        List<List<Relationship>> paths = new ArrayList<>();

        get2ContPaths(p, tsType, r, w, threshold, paths);

        if (paths.isEmpty()) return Stream.empty();

        return paths.stream().map(Result::new);
    }

    // =====================================================
    // CACHE + COMPUTE
    // =====================================================
    private CacheManager.CachedResult computeOrCached(Node source,
                                                      Node target,
                                                      String tsType,
                                                      String r,
                                                      long window,
                                                      double threshold) {

        String key = cacheKey(source, target, tsType, r, window, threshold);

        var cache = CacheManager.getCache();
        CacheManager.CachedResult cached = cache.get(key);

        if (cached != null) return cached;

        List<DataPoint> ts1 = getTimeSeries(source, tsType);
        List<DataPoint> ts2 = getTimeSeries(target, tsType);

        if (ts1.isEmpty() || ts2.isEmpty()
                || ts1.size() < window
                || ts2.size() < window) {

            cached = new CacheManager.CachedResult(null, false, -1, -1);
            cache.put(key, cached);
            return cached;
        }

        StompWithAllen st = new StompWithAllen();

        Map<String, List<double[]>> ap =
                st.myStomp(ts1, ts2, (int) window, r);

        boolean match = ap.get(r) != null && !ap.get(r).isEmpty();

        if (!match) {
            cached = new CacheManager.CachedResult(ap, false, -1, -1);
            cache.put(key, cached);
            return cached;
        }

        int[] idx = TDUtil.findIndexes(ap, r);

        cached = new CacheManager.CachedResult(ap, true, idx[0], idx[1]);
        cache.put(key, cached);

        return cached;
    }

    // =====================================================
    // SINGLE HOP
    // =====================================================
    private Relationship getSingleHop(Node source,
                                      Node target,
                                      String tsType,
                                      String r,
                                      long window,
                                      double threshold) {

        CacheManager.CachedResult cached =
                computeOrCached(source, target, tsType, r, window, threshold);

        if (!cached.match) return null;

        List<DataPoint> ts1 = getTimeSeries(source, tsType);
        List<DataPoint> ts2 = getTimeSeries(target, tsType);

        List<DataPoint> sub1 = ts1.subList(
                cached.seqAIndex,
                (int) (cached.seqAIndex + window)
        );

        List<DataPoint> sub2 = ts2.subList(
                cached.seqBIndex,
                (int) (cached.seqBIndex + window)
        );

        return new Create().vRelationshipFunction(
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
    }

    // =====================================================
    // PATH WALK (2CONT = independent hops)
    // =====================================================
    public void get2ContPaths(Path p,
                              String tsType,
                              String r,
                              long window,
                              double threshold,
                              List<List<Relationship>> paths) {

        List<Relationship> timeWalkPath = new ArrayList<>();

        Iterator<Node> it = p.nodes().iterator();
        if (!it.hasNext()) return;

        Node prev = it.next();

        while (it.hasNext()) {

            Node curr = it.next();

            Relationship rel =
                    getSingleHop(prev, curr, tsType, r, window, threshold);

            if (rel == null) return;

            counter++;
            timeWalkPath.add(rel);

            prev = curr;
        }

        if (!timeWalkPath.isEmpty()) {
            paths.add(timeWalkPath);
        }
    }
}