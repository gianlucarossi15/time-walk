package edge2Time.experiments;


import io.github.cdimascio.dotenv.Dotenv;
import kotlin.Pair;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import edge2Time.*;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;

import static edge2Time.TDUtil.*;
import static java.util.logging.Level.INFO;

public class Neo4jToParquet {
    public static void main(String[] args) throws IOException {



        Dotenv dotenv = Dotenv.load();
        var usr = dotenv.get("USERNAME");
        var pwd = dotenv.get("PASSWORD");

        final String tsType = "bmi";
        TS_JOIN ts = new TS_JOIN();
        final long window = 5;
        final String r = "overlaps";
        final double threshold = 0.85;
        final String dataset = "100";



        List<Pair<String, String>> pairs = extractPairs(usr, pwd);



        long start = System.currentTimeMillis();

        Map<String, List<DataPoint>> allTimeSeries = extractAllTimeSeries(tsType, dataset);
        List<Pair<String, String>> validTdJoinPairs = new ArrayList<>();
        for (Map.Entry<String, List<DataPoint>> entry : allTimeSeries.entrySet()) {

            List<DataPoint> sourceTs = entry.getValue();

            for (Map.Entry<String, List<DataPoint>> targetEntry : allTimeSeries.entrySet()) {


                List<DataPoint> targetTs = targetEntry.getValue();
                Map<String, List<List<Double>>> ap = ts.TS_Join(sourceTs, targetTs, window, r, threshold);
                if (ap.get(r) != null && !ap.get(r).isEmpty()) {
                    int seqAIndex = TDUtil.findIndexes(ap, r)[0];
                    int seqBIndex = TDUtil.findIndexes(ap, r)[1];
                    validTdJoinPairs.add(new Pair<>(entry.getKey(), targetEntry.getKey()));

                    List<DataPoint> sub1 = sourceTs.subList(seqAIndex, (int) (seqAIndex + window));
                    List<DataPoint> sub2 = targetTs.subList(seqBIndex, (int) (seqBIndex + window));
                }
            }
        }


        long end = System.currentTimeMillis();

        long elapsedTime = end - start;
        System.out.println("Time series extract + TS_JOIN time: " + elapsedTime + " ms ");

    }



    private static Map<String, List<DataPoint>> extractAllTimeSeries(String tsType, String dataset) throws IOException {
        Path path = new Path(String.format("/Users/gianluca/github/DottoratoCode/timeseries_%s.parquet",dataset));
        Configuration conf = new Configuration();
        Map<String, List<DataPoint>> allSeries = new HashMap<>();

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build())  {

            Group group;
            while ((group = reader.read()) != null) {
                String patientId = group.getString("patient", 0);
                String tsTimestamps = group.getValueToString(group.getType().getFieldIndex(String.format("%s_timestamps",tsType)), 0);
                String tsValues = group.getValueToString(group.getType().getFieldIndex(String.format("%s_values",tsType)), 0);
                allSeries.put(patientId, createTimeSeres(tsTimestamps, tsValues));
            }
        }
        return allSeries;
    }


    private static List<Pair<String, String>> extractPairs(String usr, String pwd){
        var url = "bolt://localhost:7687";
        try (var driver = GraphDatabase.driver(url, AuthTokens.basic(usr, pwd))) {
            driver.verifyConnectivity();
            String neo4jQuery = """
                    MATCH p = (a:Patient)-[:FATHER_OF]->(b:Patient)
                    return a.patientId as sourceNodeId, b.patientId as targetNodeId
                    """;

            EagerResult result = driver.executableQuery(neo4jQuery).execute();
            System.out.println("neo4j time: "+result.summary().resultConsumedAfter(TimeUnit.MILLISECONDS)+" ms");

            List<Pair<String, String>> pairs = new ArrayList<>();
            for (Record record : result.records()) {
                pairs.add(new Pair<>(record.get("sourceNodeId").asString(), record.get("targetNodeId").asString()));
                       
            }
            return pairs;
        }
    }
}
