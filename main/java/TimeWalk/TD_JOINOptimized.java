package edge2Time;

import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static edge2Time.TDUtil.zNormalize;

public class TD_JOINOptimized {

    public static double THRESHOLD = 0;

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    public Map<String, List<List<Double>>> TD_Join(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            long m,
            String Allen_relation,
            double threshold) {

        return computeTD_Join(T_A, T_B, (int) m, Allen_relation, threshold);
    }

    private Map<String, List<List<Double>>> computeTD_Join(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String Allen_relation,
            double threshold) {

        THRESHOLD = threshold;

        if (Allen_relation != null && !Set.of("before", "meets", "equal", "overlaps", "none").contains(Allen_relation)) {
            throw new IllegalArgumentException(Allen_relation + " is not a valid Allen's relation.");
        }

        if (!checkWindowSize(m, Math.min(T_A.size(), T_B.size()))) return new HashMap<>();

        // Rolling windows
        List<List<DataPoint>> subseq_T_A = rollingWindow(T_A, m);
        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        // Caches for lazy z-normalization
        Map<Integer, List<Double>> Z_A_cache = new HashMap<>();
        Map<Integer, List<Double>> Z_B_cache = new HashMap<>();

        // Prepare result dictionary
        Map<String, List<List<Double>>> dict = new HashMap<>();
        dict.put("before", new ArrayList<>());
        dict.put("meets", new ArrayList<>());
        dict.put("equal", new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());
        dict.put("none", new ArrayList<>());

        // Loop over subsequences
        for (int i = 0; i < subseq_T_A.size(); i++) {
            Instant startA = subseq_T_A.get(i).get(0).getTimestamp();
            Instant endA = subseq_T_A.get(i).get(m - 1).getTimestamp();

            List<List<Double>> candidateList = new ArrayList<>();

            for (int j = 0; j < subseq_T_B.size(); j++) {
                Instant startB = subseq_T_B.get(j).get(0).getTimestamp();
                Instant endB = subseq_T_B.get(j).get(m - 1).getTimestamp();

                boolean candidate = false;

                // Allen relation pruning
                switch (Allen_relation) {
                    case "before":
                        candidate = endA.isBefore(startB) && !endA.plusSeconds(1).equals(startB);
                        break;
                    case "meets":
                        candidate = endA.plusSeconds(1).equals(startB);
                        break;
                    case "equal":
                        candidate = startA.equals(startB) && endA.equals(endB);
                        break;
                    case "overlaps":
                        candidate = startA.isBefore(endB) && endA.isAfter(startB)
                                && !startA.equals(startB) && !endA.equals(endB);
                        break;
                    case "none":
                        candidate = true;
                        break;
                }

                if (!candidate) continue;

                // Lazy z-normalization
                List<Double> zA = Z_A_cache.computeIfAbsent(i, idx ->
                        zNormalize(subseq_T_A.get(idx).stream().map(DataPoint::getValue).toList())
                );
                List<Double> zB = Z_B_cache.computeIfAbsent(j, idx ->
                        zNormalize(subseq_T_B.get(idx).stream().map(DataPoint::getValue).toList())
                );

                // Euclidean distance on precomputed z-normalized vectors
                double dist = round(euclideanDistance(zA, zB), 5);
                double cosine = cosineSimilarity(zA, zB);

                if (cosine >= THRESHOLD) {
                    candidateList.add(Arrays.asList((double) i, (double) j, dist));
                }
            }

            // Add minimum distance for this subsequence
            if (Allen_relation == null || Allen_relation.equals("none")) {
                addMinDistance(candidateList, dict.get("none"));
            } else {
                addMinDistance(candidateList, dict.get(Allen_relation));
            }
        }

        return dict;
    }

    private static List<List<DataPoint>> rollingWindow(List<DataPoint> array, int windowSize) {
        int n = array.size() - windowSize + 1;
        List<List<DataPoint>> result = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            result.add(new ArrayList<>(array.subList(i, i + windowSize)));
        }
        return result;
    }

    private static boolean checkWindowSize(int m, int maxSize) {
        return m > 2 && m <= maxSize;
    }

    private static double euclideanDistance(List<Double> a, List<Double> b) {
        return Math.sqrt(IntStream.range(0, a.size()).mapToDouble(i -> Math.pow(a.get(i) - b.get(i), 2)).sum());
    }

    private static void addMinDistance(List<List<Double>> list, List<List<Double>> result) {
        if (!list.isEmpty()) {
            double minValue = list.stream().mapToDouble(a -> a.get(2)).min().orElse(Double.NaN);
            for (List<Double> item : list) {
                if (item.get(2) == minValue) {
                    result.add(Arrays.asList(item.get(0), item.get(1), round(minValue, 5)));
                    break;
                }
            }
        }
    }

    private static double cosineSimilarity(List<Double> a, List<Double> b) {
        if (a == null || b == null || a.size() != b.size())
            throw new IllegalArgumentException("Vectors must be same size");

        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.size(); i++) {
            dot += a.get(i) * b.get(i);
            normA += a.get(i) * a.get(i);
            normB += b.get(i) * b.get(i);
        }

        if (normA == 0.0 || normB == 0.0) return 0.0;
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
