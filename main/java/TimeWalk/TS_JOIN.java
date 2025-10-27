package edge2Time;

import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.IntStream;
import java.time.Instant;

import static edge2Time.TDUtil.zNormalize;

public class TS_JOIN {

    public static double THRESHOLD = 0;
    static int beforeComparisons = 0;
    static int overlapsComparisons = 0;
    static int meetsComparisons = 0;
    static int equalComparisons = 0;
//    @Procedure(name = "edge2Time.TD_JOIN")
//    @Description("Compute Time Dependent Matrix Profile TDMP.")
    public Map<String, List<List<Double>>> TS_Join(
            List<DataPoint> T_A,
            List<DataPoint> T_B,
            long m,
            String Allen_relation, double threshold) {
        return computeTS_Join(T_A, T_B, (int) m, Allen_relation, threshold);
    }
    private Map<String, List<List<Double>>> computeTS_Join(List<DataPoint> T_A, List<DataPoint> T_B, int m, String Allen_relation, double threshold) {

        beforeComparisons = 0;
        overlapsComparisons = 0;
        meetsComparisons = 0;
        equalComparisons = 0;

        T_A = new ArrayList<>(T_A);

        THRESHOLD =  threshold;

        for (DataPoint dataPoint : T_A) {
            if (Double.isInfinite(dataPoint.getValue())) {
                dataPoint.setValue(Double.NaN);
            }
        }
        if (Allen_relation != null && !Set.of("before", "meets", "equal", "overlaps","none").contains(Allen_relation))
            throw new IllegalArgumentException(Allen_relation + " is not a valid Allen's relation. Please choose from 'before', 'meets', 'equal', or 'overlaps'.");


        if (!checkWindowSize(m, Math.min(T_A.size(), T_B.size())))
             return new HashMap<>();
        List<List<DataPoint>> subseq_T_A = rollingWindow(T_A, m);
        List<List<DataPoint>> subseq_T_B = rollingWindow(T_B, m);

//        TemporalAmount tAmountT_A = TDUtil.getTimeDelta(T_A.get(0).getTimestamp());
//        TemporalAmount tAmountT_B = TDUtil.getTimeDelta(T_B.get(0).getTimestamp());
//        TemporalAmount minAmount = TDUtil.minTemporalAmount(tAmountT_A, tAmountT_B);

        Map<String, List<List<Double>>> dict = new HashMap<>();

        dict.put("before", new ArrayList<>());
        dict.put("meets", new ArrayList<>());
        dict.put("equal", new ArrayList<>());
        dict.put("overlaps", new ArrayList<>());
        dict.put("none", new ArrayList<>());

//        Map<String, Boolean> res = checkTSCompatibility(T_A, T_B, minAmount);

        // RBF check also here

        int minLength = Math.min(T_A.size(), T_B.size());
        List<Double> T_A_values = T_A.subList(0, minLength).stream().map(DataPoint::getValue).toList();
        List<Double> T_B_values = T_B.subList(0, minLength).stream().map(DataPoint::getValue).toList();

//        if (threshold !=0) {
//            List<Double> zNormalizedSubT_A = zNormalize(T_A_values);
//            List<Double> zNormalizedSubT_B = zNormalize(T_B_values);
//
//
//            TimeSeriesBase.Builder builderA = TimeSeriesBase.builder();
//            for (int i = 0; i < zNormalizedSubT_A.size(); i++) {
//                builderA.add(i, zNormalizedSubT_A.get(i));
//            }
//            TimeSeries tsA = builderA.build();  // Note: type is TimeSeries
//
//            TimeSeriesBase.Builder builderB = TimeSeriesBase.builder();
//            for (int i = 0; i < zNormalizedSubT_B.size(); i++) {
//                builderB.add(i, zNormalizedSubT_B.get(i));
//            }
//            TimeSeries tsB = builderB.build();
//
//            TimeWarpInfo dtw = FastDTW.compare(tsA, tsB, 1, Distances.EUCLIDEAN_DISTANCE);
//            double distance = dtw.getDistance();
//            int pathLength = dtw.getPath().size();
//
//
//            double maxDist = 0.0;
//            for (double a : zNormalizedSubT_A) {
//                for (double b : zNormalizedSubT_B) {
//                    double d = Math.abs(a - b); // 1D Euclidean
//                    if (d > maxDist) maxDist = d;
//                }
//            }
//
//
//            double normalizedDTW = distance / (pathLength * maxDist);
//
//            System.out.println("Normalized DTW [0,1]: " + normalizedDTW);
//
//
//            if (normalizedDTW < threshold)
//                return dict;
//
//        }

        if (Objects.equals(Allen_relation, "none")) {
//            System.out.println("No Allen's relation is provided. Computing the complete Allen profile.");
//            for (int i = 0; i < subseq_T_A.size(); i++) {
//                List<List<Double>> list_O = new ArrayList<>();
//                List<List<Double>> list_B = new ArrayList<>();
//
//                List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
//
//
//                for (int j = 0; j < subseq_T_B.size(); j++) {
//                    Instant firstInstantT_A = subseq_T_A.get(i).get(0).getTimestamp();
//                    Instant secondInstantT_A = subseq_T_A.get(i).get(m - 1).getTimestamp();
//                    Instant firstInstantT_B = subseq_T_B.get(j).get(0).getTimestamp();
//                    Instant secondInstantT_B = subseq_T_B.get(j).get(m - 1).getTimestamp();
//                    if (firstInstantT_A.equals(firstInstantT_B) && secondInstantT_A.equals(secondInstantT_B)) {
//                        List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
//                        double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);
//                        dict.get("equal").add(Arrays.asList((double) j, dist));
//                    } else if (secondInstantT_A.plusSeconds(1).equals(firstInstantT_B)) {
//                        List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
//                        double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);
//                        dict.get("meets").add(Arrays.asList((double) j, dist));
//                    } else if (!firstInstantT_A.equals(firstInstantT_B) && !secondInstantT_A.equals(secondInstantT_B) &&
//                            firstInstantT_A.isBefore(secondInstantT_B) && secondInstantT_A.isAfter(firstInstantT_B)) {
//                        List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
//                        list_O.add(Arrays.asList((double) j, zNormalizedEuclideanDistance(subT_A_values, subT_B_values)));
//                    } else if (secondInstantT_A.isBefore(firstInstantT_B) &&
//                            !secondInstantT_A.plusSeconds(1).equals(firstInstantT_B)) {
//                        List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
//                        list_B.add(Arrays.asList((double) j, zNormalizedEuclideanDistance(subT_A_values, subT_B_values)));
//                    }
//                }
//                addMinDistance(list_O, dict.get("overlaps"));
//                addMinDistance(list_B, dict.get("before"));

            for (int i = 0; i < subseq_T_A.size(); i++) {
                List<List<Double>> list = new ArrayList<>();
                for (int j = 0; j < subseq_T_B.size(); j++) {
                    List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
                    List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();

                    List<Double> zNormalizedSubT_A = zNormalize(subT_A_values);
                    List<Double> zNormalizedSubT_B = zNormalize(subT_B_values);
                    double cosine = cosineSimilarity(zNormalizedSubT_A, zNormalizedSubT_B);
                    double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);

                    if (cosine >= THRESHOLD) {
                        list.add(Arrays.asList((double) i,(double) j, dist));
                    }

                    }
                addMinDistance(list, dict.get("none"));
                }

        } else {


            switch (Allen_relation) {
                case "before":
                    computeBefore(subseq_T_A, subseq_T_B, m, dict);
                   //System.out.println("# of before comparisons: " + beforeComparisons);
                    break;
                case "meets":
                    computeMeets(subseq_T_A, subseq_T_B, m, dict);
                    //System.out.println("# of meets comparisons: " + meetsComparisons);
                    break;
                case "equal":
                    computeEqual(subseq_T_A, subseq_T_B, m, dict);
                    //System.out.println("# of equal comparisons: " + equalComparisons);
                    break;
                case "overlaps":
                    computeOverlaps(subseq_T_A, subseq_T_B, m, dict);
                    //System.out.println("# of overlaps comparisons: " + overlapsComparisons);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported Allen relation: " + Allen_relation);
            }
        }





        return dict;
    }


    private static void computeBefore(List<List<DataPoint>> subseq_T_A, List<List<DataPoint>> subseq_T_B, int m, Map<String, List<List<Double>>> dict) {
        for (int i = 0; i < subseq_T_A.size(); i++) {
            List<List<Double>> list = new ArrayList<>();
            for (int j = 0; j < subseq_T_B.size(); j++) {
                Instant secondInstantT_A = subseq_T_A.get(i).get(m - 1).getTimestamp();
                Instant firstInstantT_B = subseq_T_B.get(j).get(0).getTimestamp();
                if( secondInstantT_A.isBefore(firstInstantT_B) &&
                    !secondInstantT_A.plusSeconds(1).equals(firstInstantT_B)){

                    beforeComparisons++;

                    List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
                    List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();

                    List<Double> zNormalizedSubT_A = zNormalize(subT_A_values);
                    List<Double> zNormalizedSubT_B = zNormalize(subT_B_values);
//                    double rbfKernel = rbfKernel(zNormalizedSubT_A, zNormalizedSubT_B, SIGMA);
                    double cosine = cosineSimilarity(zNormalizedSubT_A, zNormalizedSubT_B);
                    double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);

                    if (cosine >= THRESHOLD) {
                        list.add(Arrays.asList((double) i,(double) j, dist));
//                        System.out.println("rbfKernel: " + rbfKernel);
                    }
//                    list.add(Arrays.asList((double) i,(double) j, dist));

                }
            }

            addMinDistance(list, dict.get("before"));

        }
//        if (dict.get("before").isEmpty()) {
//            System.out.println("There are no before relation with window size " + m + ". Please try a different Allen relation or window size.");
//        }
    }

    private static void computeMeets(List<List<DataPoint>> subseq_T_A, List<List<DataPoint>> subseq_T_B, int m, Map<String, List<List<Double>>> dict) {
        for (int i = 0; i < subseq_T_A.size(); i++) {
            for (int j = 0; j < subseq_T_B.size(); j++) {
                Instant secondInstantT_A = subseq_T_A.get(i).get(m - 1).getTimestamp();
                Instant firstInstantT_B = subseq_T_B.get(j).get(0).getTimestamp();
                if (secondInstantT_A.plusSeconds(1).equals(firstInstantT_B)) {
                    meetsComparisons++;
                    List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
                    List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
                    double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);

                    List<Double> zNormalizedSubT_A = zNormalize(subT_A_values);
                    List<Double> zNormalizedSubT_B = zNormalize(subT_B_values);
//                    double rbfKernel = rbfKernel(zNormalizedSubT_A, zNormalizedSubT_B,  SIGMA);
                    double cosine = cosineSimilarity(zNormalizedSubT_A, zNormalizedSubT_B);
                    if (cosine >= THRESHOLD) {
                        dict.get("meets").add(Arrays.asList((double) i, (double) j, dist));
//                        System.out.println("rbfKernel: " + rbfKernel);
                    }
//                    dict.get("meets").add(Arrays.asList((double) i, (double) j, dist));

                }
            }
        }
//        if (dict.get("meets").isEmpty()) {
//            System.out.println("There are no meets relation with window size " + m + ". Please try a different Allen relation or window size.");
//        }
    }

    private static void computeEqual(List<List<DataPoint>> subseq_T_A, List<List<DataPoint>> subseq_T_B, int m, Map<String, List<List<Double>>> dict) {
        for (int i = 0; i < subseq_T_A.size(); i++) {
            for (int j = 0; j < subseq_T_B.size(); j++) {
                Instant firstInstantT_A = subseq_T_A.get(i).get(0).getTimestamp();
                Instant secondInstantT_A = subseq_T_A.get(i).get(m - 1).getTimestamp();
                Instant firstInstantT_B = subseq_T_B.get(j).get(0).getTimestamp();
                Instant secondInstantT_B = subseq_T_B.get(j).get(m - 1).getTimestamp();

                if (firstInstantT_A.equals(firstInstantT_B) && secondInstantT_A.equals(secondInstantT_B)) {
                    equalComparisons++;
                    List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
                    List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();
                    double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);

                    List<Double> zNormalizedSubT_A = zNormalize(subT_A_values);
                    List<Double> zNormalizedSubT_B = zNormalize(subT_B_values);
//                    double rbfKernel = rbfKernel(zNormalizedSubT_A, zNormalizedSubT_B, SIGMA);
                    double cosine = cosineSimilarity(zNormalizedSubT_A, zNormalizedSubT_B);

                    if (cosine >= THRESHOLD) {
//                        System.out.println("rbfKernel: " + rbfKernel);
                        dict.get("equal").add(Arrays.asList((double) i, (double) j, dist));

                    }
//                    dict.get("equal").add(Arrays.asList((double) i, (double) j, dist));

                }
            }
        }
//        if (dict.get("equal").isEmpty()) {
//            System.out.println("There are no equal relation with window size " + m + ". Please try a different Allen relation or window size.");
//        }
    }

    private static void computeOverlaps(List<List<DataPoint>> subseq_T_A, List<List<DataPoint>> subseq_T_B, int m, Map<String, List<List<Double>>> dict) {
        for (int i = 0; i < subseq_T_A.size(); i++) {
            List<List<Double>> list = new ArrayList<>();
            for (int j = 0; j < subseq_T_B.size(); j++) {
                Instant firstInstantT_A = subseq_T_A.get(i).get(0).getTimestamp();
                Instant secondInstantT_A = subseq_T_A.get(i).get(m - 1).getTimestamp();
                Instant firstInstantT_B = subseq_T_B.get(j).get(0).getTimestamp();
                Instant secondInstantT_B = subseq_T_B.get(j).get(m - 1).getTimestamp();
                if (!firstInstantT_A.equals(firstInstantT_B) && !secondInstantT_A.equals(secondInstantT_B) && firstInstantT_A.isBefore(secondInstantT_B) && secondInstantT_A.isAfter(firstInstantT_B)){
                    overlapsComparisons++;


                    List<Double> subT_A_values = subseq_T_A.get(i).stream().map(DataPoint::getValue).toList();
                    List<Double> subT_B_values = subseq_T_B.get(j).stream().map(DataPoint::getValue).toList();

                    List<Double> zNormalizedSubT_A = zNormalize(subT_A_values);
                    List<Double> zNormalizedSubT_B = zNormalize(subT_B_values);
                    double dist = round(zNormalizedEuclideanDistance(subT_A_values, subT_B_values), 5);
                    double cosine = cosineSimilarity(zNormalizedSubT_A, zNormalizedSubT_B);

//                    double rbfKernel = rbfKernel(zNormalizedSubT_A, zNormalizedSubT_B, SIGMA);
                    if (cosine >= THRESHOLD) {
                        list.add(Arrays.asList((double) i,(double) j, dist));
//                        System.out.println("rbfKernel: " + rbfKernel);
                    }
//                    list.add(Arrays.asList((double) i,(double) j, dist));
                }
            }
            addMinDistance(list, dict.get("overlaps"));
        }
//        if (dict.get("overlaps").isEmpty()) {
//            System.out.println("There are no overlaps relation with window size " + m + ". Please try a different Allen relation or window size.");
//        }
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

    private static double zNormalizedEuclideanDistance(List<Double> a, List<Double> b) {
        List<Double> a_z = zNormalize(a);
        List<Double> b_z = zNormalize(b);
        if (a.equals(b) || isConstantList(a) || isConstantList(b))
            return euclideanDistance(a, b);
        else
            return euclideanDistance(a_z, b_z);
    }



    private static double euclideanDistance(List<Double> a, List<Double> b) {
        return Math.sqrt(IntStream.range(0, a.size()).mapToDouble(i -> Math.pow(a.get(i) - b.get(i), 2)).sum());
    }

    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    private static <T> boolean isConstantList(List<T> list) {
        if (list == null || list.size() <= 1) {
            return true;
        }
        T first = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            if (!Objects.equals(first, list.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Boolean> checkTSCompatibility(List<DataPoint> T_A, List<DataPoint> T_B, TemporalAmount minAmount) {
        Map<String, Boolean> res = new HashMap<>();
        res.put("before", true);
        res.put("meets", true);
        res.put("equal", true);
        res.put("overlaps", true);
        //the time amount that I add for the check must be the time unit of the timestamps (e.g. days, seconds, milliseconds, etc.). Change it accordingly
        if (T_A.get(T_A.size() - 1).getTimestamp().plusSeconds(1).equals(T_B.get(0).getTimestamp())) {
            res.put("before", true);
            res.put("meets", false);
            res.put("equal", false);
            res.put("overlaps", false);
        }
        return res;
    }
    public static double rbfKernel(List<Double> a, List<Double> b, double sigma) {
        if (a == null || b == null || a.size() != b.size()) {
            throw new IllegalArgumentException("Invalid input vectors");
        }

        if (isConstantList(a) && isConstantList(b) && Objects.equals(a.get(0), b.get(0))) {
            return 1.0;
        }
        // Check for NaN values
        if (a.stream().anyMatch(x -> Double.isNaN(x)) || b.stream().anyMatch(x -> Double.isNaN(x)) ||
        isConstantList(a) || isConstantList(b)) {
            double directDist = euclideanDistance(a, b);
            return Math.exp(-directDist / (2 * sigma * sigma));
        }

        double sum = 0.0;
        for (int i = 0; i < a.size(); i++) {
            double diff = a.get(i) - b.get(i);
            sum += diff * diff;
        }

        // Prevent division by zero
        if (sigma == 0) {
            return sum == 0 ? 1.0 : 0.0;
        }

        double denominator = 2 * sigma * sigma;
        return Math.exp(-sum / denominator);
    }

    private static double cosineSimilarity(List<Double> a, List<Double> b) {
        if (a == null || b == null || a.size() != b.size()) {
            throw new IllegalArgumentException("Invalid input vectors");
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < a.size(); i++) {
            dotProduct += a.get(i) * b.get(i);
            normA += a.get(i) * a.get(i);
            normB += b.get(i) * b.get(i);
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}






