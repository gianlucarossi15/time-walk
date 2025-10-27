package edge2Time.experiments;

import io.github.cdimascio.dotenv.Dotenv;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Scalability {
    public static void main(String[] args) throws IOException, InterruptedException {
        var url = "bolt://localhost:7687";
        final int ITERATION = 3;
        Dotenv dotenv = Dotenv.load();
        var usr = dotenv.get("USERNAME");
        var pwd = dotenv.get("PASSWORD");


        try (var driver = GraphDatabase.driver(url, AuthTokens.basic(usr, pwd))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");
            String delimiter = ";";

            long timestamp = System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(
                            String.format("src/main/resources/scalability_time_%d.csv", timestamp))
            );
            writer.write("dataset"+delimiter+"window"+delimiter+"relation"+delimiter+"ts"+delimiter+"query_time\n");

            int[] syntheaWindows = {4, 5, 7};
            int [] finbenchWindows = {30, 50, 70};
            String[] relations = {"overlaps", "before", "meets", "equal"};
            String[] syntheaTimeSeries = {"bmi", "bodyWeight", "bodyHeight", "heartRate", "respiratoryRate"};
            String [] finbenchTimeSeries= {"balance"};
            String[] syntheadatasetName = {"synthea_5k", "synthea_10k", "synthea_30k", "synthea_100k"};
            String[] finbenchDatasetName = {"finbench_5k","finbench_50k","finbench_160k","finbench_500k"};
            for (String name : finbenchDatasetName) {
                switch (name) {
                    case "synthea_5k" -> {
                        runDataset(relations, syntheaTimeSeries, syntheaWindows, "synthea_5k", driver, ITERATION, writer, delimiter);
                    }
                    case "synthea_10k" -> {
                        runDataset(relations, syntheaTimeSeries, syntheaWindows, "synthea_10k", driver, ITERATION, writer, delimiter);
                    }
                    case "synthea_30k" -> {
                        runDataset(relations, syntheaTimeSeries, syntheaWindows, "synthea_30k", driver, ITERATION, writer, delimiter);
                    }
                    case "synthea_100k" -> {
                        runDataset(relations, syntheaTimeSeries, syntheaWindows, "synthea_100k", driver, ITERATION, writer, delimiter);
                    }
                    case "finbench_5k" -> {
                        runDataset(relations, finbenchTimeSeries, finbenchWindows, "finbench_5k", driver, ITERATION, writer, delimiter);
                    }
                    case "finbench_50k" -> {
                        runDataset(relations, finbenchTimeSeries, finbenchWindows, "finbench_50k", driver, ITERATION, writer, delimiter);
                    }
                    case "finbench_160k" -> {
                        runDataset(relations, finbenchTimeSeries, finbenchWindows, "finbench_160k", driver, ITERATION, writer, delimiter);
                    }
                    case "finbench_500k" -> {
                        runDataset(relations, finbenchTimeSeries, finbenchWindows, "finbench_500k", driver, ITERATION, writer, delimiter);
                    }

                }
            }
            writer.close();
            System.out.println("Execution times written to file.");

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runDataset(String[] relations, String[] timeSeries, int[] windows, String dataset, Driver driver, int ITERATION, BufferedWriter writer, String delimiter) throws IOException, InterruptedException {
        ProcessBuilder pb = null;

        String datasetName="";
        if (dataset.equals("synthea_5k")) {
            pb = new ProcessBuilder("sh", "src/main/resources/synthea_5k.sh");
            datasetName="synthea";
        }
        else if (dataset.equals("synthea_10k")) {
            pb = new ProcessBuilder("sh", "src/main/resources/synthea_10k.sh");
            datasetName = "synthea";
        }
        else if (dataset.equals("synthea_30k")){
            pb = new ProcessBuilder("sh", "src/main/resources/synthea_30k.sh");
            datasetName="synthea";}

        else if (dataset.equals("synthea_100k")) {
            pb = new ProcessBuilder("sh", "src/main/resources/synthea_100k.sh");
            datasetName = "synthea";
        }
        else if (dataset.equals("finbench_5k")) {
            pb = new ProcessBuilder("sh", "src/main/resources/finbench_5k.sh");
            datasetName = "finbench";
        }
        else if (dataset.equals("finbench_50k")){
            pb = new ProcessBuilder("sh", "src/main/resources/finbench_50k.sh");
            datasetName = "finbench";
            }
        else if (dataset.equals("finbench_160k")){
            pb = new ProcessBuilder("sh", "src/main/resources/finbench_160k.sh");
            datasetName = "finbench";
        }
        else if (dataset.equals("finbench_500k")) {
            pb = new ProcessBuilder("sh", "src/main/resources/finbench_500k.sh");
            datasetName = "finbench";
        }

        pb.redirectErrorStream(true);
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("script failed with exit code " + exitCode);
        }
        String query = "";
        if(datasetName.equals("synthea"))
        query= """ 
            CYPHER RUNTIME= PARALLEL
            MATCH p=(a:Patient)-[:MOTHER_OF]->(b:Patient) CALL edge2Time.TimeWalkPath(p,"%s","%s",%d,0.85) YIELD path RETURN count(*)""";
        else {
            if (dataset.equals("finbench_5k"))
            query = """ 
                    CYPHER RUNTIME= PARALLEL
                    MATCH p=(a1:Account)-[:Transfer]->(a2:Account), (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
                    WHERE p1.country <> p2.country AND a1.balance_values IS NOT NULL AND a2.balance_values IS NOT NULL
                    LIMIT 700
                    CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
                    RETURN COUNT(*)""";
            else if(dataset.equals("finbench_50k"))
                query = """ 
                    CYPHER RUNTIME= PARALLEL
                    MATCH p=(a1:Account)-[:Transfer]->(a2:Account), (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
                    WHERE p1.country <> p2.country AND a1.balance_values IS NOT NULL AND a2.balance_values IS NOT NULL
                    LIMIT 1000
                    CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
                    RETURN COUNT(*)""";
            else if(dataset.equals("finbench_160k"))
                query = """ 
                    CYPHER RUNTIME= PARALLEL
                    MATCH p=(a1:Account)-[:Transfer]->(a2:Account), (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
                    WHERE p1.country <> p2.country AND a1.balance_values IS NOT NULL AND a2.balance_values IS NOT NULL
                    LIMIT 3000
                    CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
                    RETURN COUNT(*)""";
            else if(dataset.equals("finbench_500k"))
                query = """ 
                    CYPHER RUNTIME= PARALLEL
                    MATCH p=(a1:Account)-[:Transfer]->(a2:Account), (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
                    WHERE p1.country <> p2.country AND a1.balance_values IS NOT NULL AND a2.balance_values IS NOT NULL
                    LIMIT 5000
                    CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
                    RETURN COUNT(*)""";
        }

        for (String relation: relations){
            for (String ts: timeSeries){
                for(int w: windows){
                    long totalTime = 0;
                    for (int i = 0; i < ITERATION; i++) {
                        String newQuery = String.format(query, ts,relation,w);
                        EagerResult res = driver.executableQuery(newQuery).execute();
                        totalTime += res.summary().resultConsumedAfter(TimeUnit.MILLISECONDS);
                    }
                    long avgTime = totalTime / ITERATION;
                    writer.write(dataset +delimiter +w+ delimiter+relation+delimiter+ts+delimiter + avgTime + "\n");
                    writer.flush();

                }

            }
        }

    }
}
