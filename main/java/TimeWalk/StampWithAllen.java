package edge2Time;

import com.github.eugene.kamenev.tsmp4j.algo.mp.stamp.STAMP;

import java.time.Instant;
import java.util.*;

public class StampWithAllen {

    public Map<String, List<List<Double>>> myStamp(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            int m,
            String Allen_relation) {

        if (Allen_relation != null && !Set.of("before", "meets", "equal", "overlaps","none").contains(Allen_relation))
            throw new IllegalArgumentException(Allen_relation + " is not a valid Allen's relation. Please choose from 'before', 'meets', 'equal', or 'overlaps'.");

        List<List<DataPoint>> subseq_T_A = rollingWindow(T_A, m);
        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

        Map<String, List<List<Double>>> dict = new HashMap<>();
        dict.put("before", new ArrayList<>());
        dict.put("meets", new ArrayList<>());
        dict.put("equal", new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());

        for (int i = 0; i < subseq_T_A.size(); i++) {
            List<DataPoint> seqA = subseq_T_A.get(i);
            List<List<Double>> overlapsCandidates = new ArrayList<>();
            List<List<Double>> beforeCandidates = new ArrayList<>();

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
                        dict.get("equal").add(Arrays.asList((double) i, (double) j, dist));
                    } else if (endA.plusSeconds(1).equals(startB)) { // meets
                        dict.get("meets").add(Arrays.asList((double) i, (double) j, dist));
                    } else if (startA.isBefore(endB) && endA.isAfter(startB)) { // overlaps
                        overlapsCandidates.add(Arrays.asList((double) i, (double) j, dist));
                    } else if (endA.isBefore(startB)) { // before
                        beforeCandidates.add(Arrays.asList((double) i, (double) j, dist));
                    }
                } else {
                    switch (Allen_relation) {
                        case "equal":
                            if (startA.equals(startB) && endA.equals(endB)) {
                                dict.get("equal").add(Arrays.asList((double) i, (double) j, dist));
                            }
                            break;
                        case "meets":
                            if (endA.plusSeconds(1).equals(startB)) {
                                dict.get("meets").add(Arrays.asList((double) i, (double) j, dist));
                            }
                            break;
                        case "overlaps":
                            if (startA.isBefore(endB) && endA.isAfter(startB) && !startA.equals(startB)) {
                                overlapsCandidates.add(Arrays.asList((double) i, (double) j, dist));
                            }
                            break;
                        case "before":
                            if (endA.isBefore(startB)) {
                                beforeCandidates.add(Arrays.asList((double) i, (double) j, dist));
                            }
                            break;
                    }
                }
            }
            addMinDistance(overlapsCandidates, dict.get("overlaps"));
            addMinDistance(beforeCandidates, dict.get("before"));

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
    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }
}
