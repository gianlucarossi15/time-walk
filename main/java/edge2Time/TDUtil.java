package edge2Time;

import org.neo4j.graphdb.Node;

import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.Instant;

public final class TDUtil {
    public static List<DataPoint> createTimeSeres(Object tsTime, Object tsValue) {
        double[] tsValueArray;
        Instant[] tsTimeArray;

        tsValueArray = TDUtil.parseTsValue(tsValue);
        tsTimeArray = TDUtil.parseTsTime(tsTime);

        List<Double> tsV = Arrays.stream(tsValueArray).boxed().toList();
        List<Instant> tsT = Arrays.stream(tsTimeArray).toList();
        List<DataPoint> timeSeries = new ArrayList<>();
        for (int i = 0; i < tsT.size(); i++) {
            timeSeries.add(new DataPoint(tsT.get(i).toString(), tsV.get(i)));
        }
        return timeSeries;
    }

    public static double[] parseTsValue(Object tsValue) {
        if (tsValue instanceof String[]) {
            return Arrays.stream((String[]) tsValue)
                    .mapToDouble(Double::parseDouble)
                    .toArray();
        } else if (tsValue instanceof double[]) {
            return (double[]) tsValue;
        } else if (tsValue instanceof String) {
            // Use the helper for JSON-like array strings
            List<Double> values = parseJsonArrayOfDoubles((String) tsValue);
            return values.stream().mapToDouble(Double::doubleValue).toArray();
        } else {
            throw new IllegalArgumentException("Unsupported property type for tsValue: " + tsValue.getClass());
        }
    }


    public static List<Double> parseJsonArrayOfDoubles(String jsonArray) {
        jsonArray = jsonArray.replaceAll("[\\[\\]\"]", ""); // Remove brackets and quotes
        String[] parts = jsonArray.split(",");
        List<Double> result = new ArrayList<>();
        for (String part : parts) {
            if (!part.trim().isEmpty()) {
                result.add(Double.parseDouble(part.trim()));
            }
        }
        return result;
    }

    public static Instant[] parseTsTime(Object tsTime) {
        if (tsTime instanceof ZonedDateTime[]) {
            return Arrays.stream((ZonedDateTime[]) tsTime)
                    .map(ZonedDateTime::toInstant)
                    .toArray(Instant[]::new);
        } else if (tsTime instanceof Instant[]) {
            return (Instant[]) tsTime;
        } else if (tsTime instanceof String[]) {
            return Arrays.stream((String[]) tsTime)
                    .map(Instant::parse)
                    .toArray(Instant[]::new);
        } else if (tsTime instanceof String) {
            String[] parts = parseJsonArrayOfStrings((String) tsTime);
            return Arrays.stream(parts)
                    .map(Instant::parse)
                    .toArray(Instant[]::new);
        } else {
            throw new IllegalArgumentException("Unsupported property type for tsTime: " + tsTime.getClass());
        }
    }

    public static String[] parseJsonArrayOfStrings(String jsonArray) {
        jsonArray = jsonArray.replaceAll("[\\[\\]\"]", ""); // Remove brackets and quotes
        String[] parts = jsonArray.split(",");
        return Arrays.stream(parts)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }



    public static int[] findIndexes(Map<String, List<List<Double>>> joinRes, String r) {
        List<List<Double>> resultList = joinRes.get(r);
        if (resultList == null || resultList.isEmpty()) {
            throw new IllegalArgumentException("No join results found for key: " + r);
        }
        List<Double> values = resultList.stream()
                .map(item -> item.get(2))
                .toList();

        // Find the index of the minimum value
        int index = IntStream.range(0, values.size())
                .reduce((i, j) -> values.get(i) < values.get(j) ? i : j)
                .orElse(-1);

        if (index == -1) {
            throw new IllegalStateException("Unable to find minimum value index for key: " + r);
        }

        // Get the first element (item[0]) corresponding to the minimum value
        int seq_A_index = resultList.get(index).get(0).intValue();
        int seq_B_index = resultList.get(index).get(1).intValue();

        return new int[]{seq_A_index, seq_B_index};
    }
    public static IntermediateResult findIntermediateResult(Map<String, IntermediateResult> results, Node current, Node next) {
        String pairKey = current.getElementId() + "-" + next.getElementId();
        return results.get(pairKey);
    }

    /**
     * Helper method to obtain the time series as a list of DataPoint.
     */
    public static List<DataPoint> getTimeSeries(Node node, String tsType) {
        Object tsTime = node.getProperty(tsType + "_timestamps", null);
        Object tsValue = node.getProperty(tsType + "_values", null);
        if (tsTime == null || tsValue == null) {
            return Collections.emptyList();
        }
        return TDUtil.createTimeSeres(tsTime, tsValue);
    }
//    public static TemporalAmount getTimeDelta(Instant instant) {
//        return instant.
//    }
//    public static TemporalAmount minTemporalAmount(TemporalAmount a, TemporalAmount b) {
//        // Order: seconds < minutes < hours < days < months < years
//        int aOrder = getGranularityOrder(a);
//        int bOrder = getGranularityOrder(b);
//        return (aOrder < bOrder) ? a : b;
//    }
//
//    public static int getGranularityOrder(TemporalAmount t) {
//        if (t instanceof Duration) {
//            Duration d = (Duration) t;
//            if (d.getSeconds() == 1) return 0; // seconds
//            if (d.toMinutes() == 1) return 1; // minutes
//            if (d.toHours() == 1) return 2; // hours
//            if (d.toDays() == 1) return 3; // days
//        } else if (t instanceof Period) {
//            Period p = (Period) t;
//            if (p.getYears() == 0 && p.getMonths() == 1) return 4; // months
//            if (p.getYears() == 1) return 5; // years
//        }
//        return Integer.MAX_VALUE; // unknown, treat as coarsest
//    }


    public static List<Double> zNormalize(List<Double> array) {
        double mean = array.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
        double std = Math.sqrt(array.stream().mapToDouble(x -> Math.pow(x - mean, 2)).average().orElse(Double.NaN));
        return array.stream().map(x -> (x - mean) / std).collect(Collectors.toList());
    }

}
