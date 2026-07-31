package TimeWalk;

import com.github.eugene.kamenev.tsmp4j.algo.mp.stamp.STAMP;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class StampWithAllen {

    public Map<String, List<double[]>> myStamp(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String Allen_relation) {

        if (Allen_relation != null && !Set.of("before", "meets", "equal", "overlaps","none").contains(Allen_relation))
            throw new IllegalArgumentException(Allen_relation + " is not a valid Allen's relation. Please choose from 'before', 'meets', 'equal', or 'overlaps'.");

        // ---- Compatibility check (Definition 6, Compatible Time Series) --
        //
        // Two time series are compatible iff their granularity unit
        // matches (e.g. both HOURS, regardless of exact sampling rate
        // within that unit). Allen relations on a shared grid are
        // ill-defined otherwise, and meets/equal's direct-index fast
        // path additionally relies on both series sharing a regular
        // grid — so we check both.
//        GranularityUnit unitA = granularityUnitOf(T_A);
//        GranularityUnit unitB = granularityUnitOf(T_B);
//        if (unitA != unitB) {
//            throw new IllegalArgumentException(
//                    "Incompatible time series: granularity unit " + unitA +
//                            " on T_A vs " + unitB + " on T_B");
//        }

        Map<String, List<double[]>> dict = new HashMap<>();
        dict.put("before", new ArrayList<>());
        dict.put("meets", new ArrayList<>());
        dict.put("equal", new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());

        // ---------------------------------------------------------------
        // FAST PATH: meets / equal admit a direct index computation.
        // No (i,j) search, no STAMP call per pair — compute the unique
        // candidate j for each i directly from the time grid, and only
        // run STAMP on that single candidate (if any).
        //   - free (this method): O(L_A - w + 1) STAMP calls at most.
        //   - pinned (myStampPinned): O(1) STAMP calls at most.
        // ---------------------------------------------------------------
        if ("meets".equals(Allen_relation) || "equal".equals(Allen_relation)) {
            computeDirectIndexJoin(T_A, T_B, m, Allen_relation, false, dict);
            return dict;
        }

        // ---------------------------------------------------------------
        // SLOW PATH (unchanged): before / overlaps / none need a real
        // range scan since many (i,j) pairs can satisfy the relation.
        // ---------------------------------------------------------------

        List<List<DataPoint>> subseq_T_A = rollingWindow(T_A, m);
        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        for (int i = 0; i < subseq_T_A.size(); i++) {
            List<DataPoint> seqA = subseq_T_A.get(i);
            List<double[]> overlapsCandidates = new ArrayList<>();
            List<double[]> beforeCandidates = new ArrayList<>();

            for (int j = 0; j < subseq_T_B.size(); j++) {
                List<DataPoint> seqB = subseq_T_B.get(j);

                Instant startA = seqA.get(0).getTimestamp();
                Instant endA = seqA.get(m - 1).getTimestamp();
                Instant startB = seqB.get(0).getTimestamp();
                Instant endB = seqB.get(m - 1).getTimestamp();

                // compute distance using STAMP
                double[] seqAValues = seqA.stream().mapToDouble(DataPoint::getValue).toArray();
                double[] seqBValues = seqB.stream().mapToDouble(DataPoint::getValue).toArray();
                var mp = STAMP.of(seqAValues, seqBValues, m);

                double dist = Arrays.stream(mp.profile()).min().orElse(Double.POSITIVE_INFINITY);

                if (Allen_relation == null) {
                    if (startA.equals(startB) && endA.equals(endB)) { // equal
                        dict.get("equal").add(new double[]{i, j, dist});
                    } else if (endA.plusSeconds(1).equals(startB)) { // meets
                        dict.get("meets").add(new double[]{i, j, dist});
                    } else if (startA.isBefore(endB) && endA.isAfter(startB)) { // overlaps
                        overlapsCandidates.add(new double[]{i, j, dist});
                    } else if (endA.isBefore(startB)) { // before
                        beforeCandidates.add(new double[]{i, j, dist});
                    }
                } else {
                    switch (Allen_relation) {
                        case "overlaps":
                            if (startA.isBefore(endB) && endA.isAfter(startB) && !startA.equals(startB)) {
                                overlapsCandidates.add(new double[]{i, j, dist});
                            }
                            break;
                        case "before":
                            if (endA.isBefore(startB)) {
                                beforeCandidates.add(new double[]{i, j, dist});
                            }
                            break;
                        default:
                            // meets/equal never reach here (handled by fast path above)
                            break;
                    }
                }
            }

            // Add minimum distance candidates
            addMinDistance(overlapsCandidates, dict.get("overlaps"));
            addMinDistance(beforeCandidates, dict.get("before"));
        }

        return dict;
    }

    // ------------------------------------------------------------------
    //  Pinned-source STAMP join (CONT recursive hop, hop i >= 2)
    //
    //  pinnedSource is the exact source subsequence (length == m),
    //  equal to I_{i-1}^{(2)} from the previous hop. Only the target
    //  window over T_B slides; the source never moves.
    //
    //  The returned source index in every result tuple is always 0.
    //  The caller (StampTimeWalkPathCont.getSingleHopPinned) remaps it
    //  to the absolute position in the original series.
    // ------------------------------------------------------------------

    public Map<String, List<double[]>> myStampPinned(
            List<DataPoint> pinnedSource,
            List<DataPoint> T_B,
            int m,
            String Allen_relation) {

        if (pinnedSource.size() != m)
            throw new IllegalArgumentException(
                    "pinnedSource length " + pinnedSource.size() +
                            " does not match window size " + m);

        if (Allen_relation != null && !Set.of("before", "meets", "equal", "overlaps", "none").contains(Allen_relation))
            throw new IllegalArgumentException(Allen_relation + " is not a valid Allen's relation. Please choose from 'before', 'meets', 'equal', or 'overlaps'.");

        // ---- Compatibility check (commented, see myStamp) ----------------
//        GranularityUnit unitA = granularityUnitOf(pinnedSource);
//        GranularityUnit unitB = granularityUnitOf(T_B);
//        if (unitA != unitB) {
//            throw new IllegalArgumentException(
//                    "Incompatible time series: granularity unit " + unitA +
//                            " on pinnedSource vs " + unitB + " on T_B");
//        }

        Map<String, List<double[]>> dict = new HashMap<>();
        dict.put("before", new ArrayList<>());
        dict.put("meets", new ArrayList<>());
        dict.put("equal", new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());

        // ---------------------------------------------------------------
        // FAST PATH: meets / equal — O(1). Only the single pinned source
        // window is involved (no outer loop), and the matching target
        // index is computed directly instead of scanning subseq_T_B.
        // ---------------------------------------------------------------
        if ("meets".equals(Allen_relation) || "equal".equals(Allen_relation)) {
            computeDirectIndexJoin(pinnedSource, T_B, m, Allen_relation, true, dict);
            return dict;
        }

        // ---------------------------------------------------------------
        // SLOW PATH (unchanged): before / overlaps / none.
        // ---------------------------------------------------------------

        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        // pinnedSource is the single source window — no outer loop, i = 0 always.
        double[] seqAValues = pinnedSource.stream().mapToDouble(DataPoint::getValue).toArray();

        Instant startA = pinnedSource.get(0).getTimestamp();
        Instant endA   = pinnedSource.get(m - 1).getTimestamp();

        List<double[]> overlapsCandidates = new ArrayList<>();
        List<double[]> beforeCandidates   = new ArrayList<>();

        for (int j = 0; j < subseq_T_B.size(); j++) {
            List<DataPoint> seqB = subseq_T_B.get(j);

            Instant startB = seqB.get(0).getTimestamp();
            Instant endB   = seqB.get(m - 1).getTimestamp();

            // compute distance using STAMP — identical to myStamp
            double[] seqBValues = seqB.stream().mapToDouble(DataPoint::getValue).toArray();
            var mp = STAMP.of(seqAValues, seqBValues, m);
            double dist = Arrays.stream(mp.profile()).min().orElse(Double.POSITIVE_INFINITY);

            if (Allen_relation == null) {
                if (startA.equals(startB) && endA.equals(endB)) {
                    dict.get("equal").add(new double[]{0, j, dist});
                } else if (endA.plusSeconds(1).equals(startB)) {
                    dict.get("meets").add(new double[]{0, j, dist});
                } else if (startA.isBefore(endB) && endA.isAfter(startB)) {
                    overlapsCandidates.add(new double[]{0, j, dist});
                } else if (endA.isBefore(startB)) {
                    beforeCandidates.add(new double[]{0, j, dist});
                }
            } else {
                switch (Allen_relation) {
                    case "overlaps":
                        if (startA.isBefore(endB) && endA.isAfter(startB) && !startA.equals(startB)) {
                            overlapsCandidates.add(new double[]{0, j, dist});
                        }
                        break;
                    case "before":
                        if (endA.isBefore(startB)) {
                            beforeCandidates.add(new double[]{0, j, dist});
                        }
                        break;
                    default:
                        // meets/equal never reach here (handled by fast path above)
                        break;
                }
            }
        }

        addMinDistance(overlapsCandidates, dict.get("overlaps"));
        addMinDistance(beforeCandidates, dict.get("before"));

        return dict;
    }

    // ------------------------------------------------------------------
    //  Direct index computation for meets / equal.
    //
    //  Both relations are defined purely by an exact timestamp condition
    //  between window i in T_A and window j in T_B:
    //    - meets : endA + 1s == startB   (matches the original's
    //              endA.plusSeconds(1) semantics, unchanged)
    //    - equal : startA == startB && endA == endB
    //
    //  Given a regular grid, the matching j is a deterministic function
    //  of i — so we look it up via a timestamp->index map instead of
    //  scanning all j for each i, and we only invoke the (expensive)
    //  STAMP matrix-profile call for that single candidate, instead of
    //  for every (i,j) pair.
    //
    //  Complexity:
    //    - building startIndexB: O(L_B - w + 1)
    //    - pinned (nA = 1):       O(1) STAMP calls
    //    - free   (nA = L_A-w+1): O(L_A - w + 1) STAMP calls
    // ------------------------------------------------------------------

    private void computeDirectIndexJoin(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String relation,
            boolean pinned,
            Map<String, List<double[]>> dict) {

        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        // Build a timestamp -> start-index map for T_B's rolling windows.
        // O(L_B) to build, reused across all i.
        int nB = subseq_T_B.size();
        // Size for nB entries without triggering a resize: capacity must
        // satisfy capacity * loadFactor(0.75) >= nB.
        Map<Instant, Integer> startIndexB = new HashMap<>((int) (nB / 0.75f) + 1);
        for (int j = 0; j < nB; j++) {
            startIndexB.put(subseq_T_B.get(j).get(0).getTimestamp(), j);
        }

        List<List<DataPoint>> subseq_T_A = pinned
                ? List.of(new ArrayList<>(T_A))   // pinned: T_A itself is the single window
                : rollingWindow(T_A, m);

        List<double[]> candidateList = new ArrayList<>();

        for (int i = 0; i < subseq_T_A.size(); i++) {
            List<DataPoint> seqA = subseq_T_A.get(i);
            Instant startA = seqA.get(0).getTimestamp();
            Instant endA   = seqA.get(m - 1).getTimestamp();

            // Direct computation of the unique target start timestamp —
            // O(1) arithmetic, no inner loop over T_B.
            Instant wantedStartB = "meets".equals(relation)
                    ? endA.plusSeconds(1)   // meets: matches original's +1s semantics
                    : startA;               // equal: startB = startA

            Integer j = startIndexB.get(wantedStartB);
            if (j == null) continue; // no exact alignment on the grid -> no match for this i

            List<DataPoint> seqB = subseq_T_B.get(j);

            if ("equal".equals(relation)) {
                Instant endB = seqB.get(m - 1).getTimestamp();
                if (!endB.equals(endA)) continue;
            }

            // Only now do we pay for the STAMP matrix-profile computation —
            // at most once per i, instead of once per (i,j) pair.
            double[] seqAValues = seqA.stream().mapToDouble(DataPoint::getValue).toArray();
            double[] seqBValues = seqB.stream().mapToDouble(DataPoint::getValue).toArray();
            var mp = STAMP.of(seqAValues, seqBValues, m);
            double dist = Arrays.stream(mp.profile()).min().orElse(Double.POSITIVE_INFINITY);

            candidateList.add(new double[]{pinned ? 0 : i, j, dist});
        }

        addMinDistance(candidateList, dict.get(relation));
    }

    private static List<List<DataPoint>> rollingWindow(List<DataPoint> array, int windowSize) {
        int n = array.size() - windowSize + 1;
        List<List<DataPoint>> result = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            result.add(new ArrayList<>(array.subList(i, i + windowSize)));
        }
        return result;
    }

    // ------------------------------------------------------------------
    //  Compatibility — granularity unit (kept for parity with
    //  TD_JOINOptimized; commented out above, same as there)
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

    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }
}