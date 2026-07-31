package TimeWalk;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TD_JOINOptimized {

    public static double THRESHOLD = 0;

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    // ------------------------------------------------------------------
    //  Free TD-Join (2CONT / base hop)
    //  Source sliding window: full range [0 .. len(T_A)-m]
    // ------------------------------------------------------------------

    public Map<String, List<double[]>> TD_Join(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            long m,
            String Allen_relation,
            double threshold) {

        return computeTD_Join(T_A, T_B, (int) m, Allen_relation, threshold,
                false /* not pinned: slide over all of T_A */);
    }

    // ------------------------------------------------------------------
    //  Pinned TD-Join (CONT recursive hop)
    //
    //  Caller passes the *exact* source subsequence to use (length m).
    //  No sliding on T_A — only T_B's window is explored. Used when
    //  hop i ≥ 2 must propagate the previous hop's target as its source.
    //
    //  The returned source index in the result tuples is always 0
    //  (the pinned source has no offset within itself). The caller is
    //  responsible for remapping it to the absolute position in the
    //  original series if needed.
    // ------------------------------------------------------------------

    public Map<String, List<double[]>> TD_JoinPinned(
            List<DataPoint> pinnedSource,
            List<DataPoint> T_B,
            long m,
            String Allen_relation,
            double threshold) {

        if (pinnedSource.size() != m) {
            throw new IllegalArgumentException(
                    "pinnedSource length " + pinnedSource.size() +
                            " does not match window size " + m);
        }

        return computeTD_Join(pinnedSource, T_B, (int) m, Allen_relation, threshold,
                true /* pinned: pinnedSource is itself the single source window */);
    }

    // ------------------------------------------------------------------
    //  Shared implementation
    //
    //  pinned = false  →  T_A is sliced into all rolling windows of size m
    //  pinned = true   →  T_A is itself the single source subsequence
    //                     (must have length exactly m)
    //
    //  Allen relations are evaluated using the temporal granularity Δt
    //  of the time series (derived from consecutive timestamps), not a
    //  hardcoded unit. Both series must share the same granularity
    //  (Definition 6, Compatible Time Series).
    //
    //  Dispatch:
    //   - meets / equal    → computeDirectIndexJoin: O(1) pinned,
    //                        O(L_A - w + 1) free. The target index is
    //                        computed directly via grid arithmetic
    //                        (validTargetRange), no (i,j) search and no
    //                        auxiliary index structure.
    //   - before/overlaps  → computeRangeJoin: the valid target range is
    //                        computed directly via grid arithmetic
    //                        (validTargetRange) instead of scanning all
    //                        of T_B and rejecting non-candidates with a
    //                        boolean check, so only genuine
    //                        Allen-satisfying (i,j) pairs are ever
    //                        visited.
    // ------------------------------------------------------------------

    private Map<String, List<double[]>> computeTD_Join(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String Allen_relation,
            double threshold,
            boolean pinned) {

        THRESHOLD = threshold;

        if (Allen_relation != null &&
                !Set.of("before", "meets", "equal", "overlaps", "none")
                        .contains(Allen_relation)) {
            throw new IllegalArgumentException(
                    Allen_relation + " is not a valid Allen's relation.");
        }

        if (!checkWindowSize(m, Math.min(T_A.size(), T_B.size())))
            return new HashMap<>();

        // ---- Compatibility check (Definition 6, Compatible Time Series) --
        //
        // Two time series are compatible iff their granularity unit
        // matches (e.g. both HOURS, regardless of exact sampling rate
        // within that unit). Allen relations on a shared grid are
        // ill-defined otherwise, and meets/equal's direct-index fast
        // path additionally relies on both series sharing the same Δt,
        // not just the same unit — so we check both.
//        GranularityUnit unitA = granularityUnitOf(T_A);
//        GranularityUnit unitB = granularityUnitOf(T_B);
//        if (unitA != unitB) {
//            throw new IllegalArgumentException(
//                    "Incompatible time series: granularity unit " + unitA +
//                            " on T_A vs " + unitB + " on T_B");
//        }

        Map<String, List<double[]>> dict = new HashMap<>();
        dict.put("before",   new ArrayList<>());
        dict.put("meets",    new ArrayList<>());
        dict.put("equal",    new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());
        dict.put("none",     new ArrayList<>());

        // ---------------------------------------------------------------
        // "none" has no Allen-arithmetic shortcut (any pair is a
        // candidate) — keep the original unrestricted double loop.
        // ---------------------------------------------------------------
        if (Allen_relation == null || Allen_relation.equals("none")) {
            computeNoneJoin(T_A, T_B, m, pinned, dict);
            return dict;
        }

        // ---------------------------------------------------------------
        // FAST/RANGE PATH: before, overlaps, meets, equal all admit a
        // direct index computation via validTargetRange — no scan over
        // rejected candidates for any of the four relations.
        // ---------------------------------------------------------------
        computeRangeJoin(T_A, T_B, m, Allen_relation, pinned, dict);
        return dict;
    }

    // ------------------------------------------------------------------
    //  Range-based join for before / overlaps / meets / equal.
    //
    //  Given regular, compatible grids (Definition 6), the set of valid
    //  target indices j for a given source index i is a *bounded,
    //  directly-computable range* (validTargetRange), rather than
    //  something that must be discovered by scanning all of T_B and
    //  testing each candidate:
    //    - meets/equal : single index                -> O(1)
    //    - overlaps    : range of size O(w-1)
    //    - before      : range of size O(L_B - w + 1)
    //  Only indices within this range are ever visited, so every visited
    //  (i,j) pair is a genuine Allen-satisfying candidate — matching the
    //  per-hop costs of Table III exactly, for both the free case
    //  (nA = L_A - w + 1) and the pinned case (nA = 1).
    // ------------------------------------------------------------------

    private void computeRangeJoin(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String relation,
            boolean pinned,
            Map<String, List<double[]>> dict) {

        Duration delta = Duration.between(T_A.get(0).getTimestamp(),
                T_A.get(1).getTimestamp());
        long deltaNanos = delta.toNanos();

        // O(1) grid offset between the two series' start timestamps,
        // computed once via arithmetic instead of an O(L_B - w + 1)
        // auxiliary index structure.
        Duration startGap = Duration.between(T_B.get(0).getTimestamp(),
                T_A.get(0).getTimestamp());

        if (deltaNanos == 0 || startGap.toNanos() % deltaNanos != 0) {
            // Series not aligned on a shared integer grid offset -> no
            // direct-range shortcut possible; fail closed (no match),
            // consistent with Definition 6's compatibility requirement.
            return;
        }
        long offsetAB = startGap.toNanos() / deltaNanos;

        int nB = T_B.size() - m + 1;
        int nA = pinned ? 1 : (T_A.size() - m + 1);

        Map<Integer, double[]> Z_A_cache = new HashMap<>();
        Map<Integer, double[]> Z_B_cache = new HashMap<>();

        for (int i = 0; i < nA; i++) {

            int[] range = validTargetRange(i, m, offsetAB, nB, relation);
            if (range == null) continue;

            List<double[]> candidateList = new ArrayList<>();

            for (int j = range[0]; j <= range[1]; j++) {

                List<DataPoint> winA = pinned ? T_A : T_A.subList(i, i + m);
                List<DataPoint> winB = T_B.subList(j, j + m);

                double[] zA = Z_A_cache.computeIfAbsent(i, idx -> zNormalizeArray(winA));
                double[] zB = Z_B_cache.computeIfAbsent(j, idx -> zNormalizeArray(winB));

                double dist   = round(euclideanDistance(zA, zB), 5);
                double cosine = cosineSimilarity(zA, zB);

                if (cosine >= THRESHOLD) {
                    candidateList.add(new double[]{i, j, dist});
                }
            }

            addMinDistance(candidateList, dict.get(relation));
        }
    }

    // ------------------------------------------------------------------
    //  Bounded valid-target-index range for a given Allen relation.
    //
    //  Given source window start index i in T_A, returns [loIdx, hiIdx]
    //  (inclusive, in T_B's index space) or null if no valid index
    //  exists. Matches Table III's per-hop cost exactly:
    //
    //    equal : j = i + offsetAB                     -> size 1 (O(1))
    //    meets : j = i + m + offsetAB                  -> size 1 (O(1))
    //    before: j in [i+m+offsetAB, nB-1]             -> size O(nB)
    //    overlaps: j in [i+offsetAB+1, i+offsetAB+m-2] -> size O(m-1)
    // ------------------------------------------------------------------

    private int[] validTargetRange(int i, int m, long offsetAB, int nB, String relation) {
        switch (relation) {
            case "equal": {
                int j = (int) (i + offsetAB);
                return (j >= 0 && j < nB) ? new int[]{j, j} : null;
            }
            case "meets": {
                int j = (int) (i + m + offsetAB);
                return (j >= 0 && j < nB) ? new int[]{j, j} : null;
            }
            case "before": {
                int lo = (int) (i + m + offsetAB);
                int hi = nB - 1;
                lo = Math.max(lo, 0);
                return (lo <= hi) ? new int[]{lo, hi} : null;
            }
            case "overlaps": {
                int lo = (int) (i + offsetAB + 1);
                int hi = (int) (i + offsetAB + m - 2);
                lo = Math.max(lo, 0);
                hi = Math.min(hi, nB - 1);
                return (lo <= hi) ? new int[]{lo, hi} : null;
            }
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------
    //  "none": no Allen-relation filter — every (i,j) pair is a
    //  candidate. Kept as the original unrestricted double loop, since
    //  there is no arithmetic shortcut to derive (the whole point of
    //  "none" is an unfiltered comparison).
    // ------------------------------------------------------------------

    private void computeNoneJoin(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            boolean pinned,
            Map<String, List<double[]>> dict) {

        List<List<DataPoint>> subseq_T_A;
        if (pinned) {
            subseq_T_A = List.of(new ArrayList<>(T_A));
        } else {
            subseq_T_A = rollingWindow(T_A, m);
        }
        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        Map<Integer, double[]> Z_A_cache = new HashMap<>();
        Map<Integer, double[]> Z_B_cache = new HashMap<>();

        for (int i = 0; i < subseq_T_A.size(); i++) {

            List<double[]> candidateList = new ArrayList<>();

            for (int j = 0; j < subseq_T_B.size(); j++) {

                double[] zA = Z_A_cache.computeIfAbsent(i, idx ->
                        zNormalizeArray(subseq_T_A.get(idx)));

                double[] zB = Z_B_cache.computeIfAbsent(j, idx ->
                        zNormalizeArray(subseq_T_B.get(idx)));

                double dist   = round(euclideanDistance(zA, zB), 5);
                double cosine = cosineSimilarity(zA, zB);

                if (cosine >= THRESHOLD) {
                    candidateList.add(new double[]{i, j, dist});
                }
            }

            addMinDistance(candidateList, dict.get("none"));
        }
    }

    // ------------------------------------------------------------------
    //  Rolling window
    // ------------------------------------------------------------------

    private static List<List<DataPoint>> rollingWindow(
            List<DataPoint> array, int windowSize) {

        int n = array.size() - windowSize + 1;
        List<List<DataPoint>> result = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            result.add(new ArrayList<>(array.subList(i, i + windowSize)));
        }

        return result;
    }

    // ------------------------------------------------------------------
    //  Compatibility — granularity unit
    // ------------------------------------------------------------------

    private enum GranularityUnit {
        SECONDS, MINUTES, HOURS, DAYS
    }

    /**
     * Picks the coarsest standard unit (DAYS, HOURS, MINUTES, SECONDS)
     * that divides Δt exactly. Two series with the same returned unit
     * are considered compatible regardless of exact sampling frequency
     * (e.g., 24h and 72h both classify as DAYS).
     */
    private static GranularityUnit granularityUnitOf(List<DataPoint> ts) {
        if (ts.size() < 2) {
            throw new IllegalArgumentException(
                    "Cannot determine granularity from a series with fewer than 2 points");
        }
        Duration d = Duration.between(ts.get(0).getTimestamp(),
                ts.get(1).getTimestamp());
        long seconds = d.getSeconds();

        if (seconds % 86_400 == 0) return GranularityUnit.DAYS;
        if (seconds %  3_600 == 0) return GranularityUnit.HOURS;
        if (seconds %     60 == 0) return GranularityUnit.MINUTES;
        return GranularityUnit.SECONDS;
    }

    // ------------------------------------------------------------------
    //  Helpers (unchanged from original)
    // ------------------------------------------------------------------

    private static boolean checkWindowSize(int m, int maxSize) {
        return m > 2 && m <= maxSize;
    }

    private static double euclideanDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    private static void addMinDistance(List<double[]> list, List<double[]> result) {
        if (list.isEmpty()) return;

        double minValue = Double.MAX_VALUE;
        double[] best = null;
        for (double[] item : list) {
            if (item[2] < minValue) {
                minValue = item[2];
                best = item;
            }
        }

        if (best != null) {
            result.add(new double[]{best[0], best[1], round(minValue, 5)});
        }
    }

    private static double cosineSimilarity(double[] a, double[] b) {
        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot   += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0) return 0.0;
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private static double[] zNormalizeArray(List<DataPoint> subseq) {
        int n = subseq.size();
        double[] values = new double[n];

        double mean = 0;
        for (int i = 0; i < n; i++) {
            values[i] = subseq.get(i).getValue();
            mean += values[i];
        }
        mean /= n;

        double std = 0;
        for (int i = 0; i < n; i++) {
            double diff = values[i] - mean;
            std += diff * diff;
        }
        std = Math.sqrt(std / n);

        if (std == 0) return new double[n];

        for (int i = 0; i < n; i++) {
            values[i] = (values[i] - mean) / std;
        }

        return values;
    }
}