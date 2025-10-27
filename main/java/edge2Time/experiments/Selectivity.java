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

public class Selectivity {
    public static final String [][] finbenchMirroredQueries = {
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        WHERE p1.country <> p2.country
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        WHERE p1.country <> p2.country
        CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer]->(a2:Account)
        CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
        RETURN COUNT(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer|Withdraw]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        RETURN COUNT(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(a1:Account)-[:Transfer|Withdraw]->(a2:Account),
        (p1:Person)-[:Own]->(a1), (a2)<-[:Own]-(p2:Person)
        CALL edge2Time.TimeWalkPath(p, "%s", "%s", %d, 0.85) YIELD path
        RETURN COUNT(*)
        """
            }
    };

    public static final String [][] syntheaMirroredQueries = {
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF]->(b:Patient) CALL edge2Time.TimeWalkPath(p,"%s","%s",%d,0.85) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:MOTHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:MOTHER_OF]->(b:Patient) CALL edge2Time.TimeWalkPath(p,"%s","%s",%d,0.85) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF*2]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF*2]->(b:Patient) CALL edge2Time.TimeWalkPath(p,"%s","%s",%d,0.85) YIELD path RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF|WAS_CLASSMATE_OF|MOTHER_OF]->(b:Patient) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL MATCH p=(a:Patient)-[:FATHER_OF|WAS_CLASSMATE_OF|MOTHER_OF]->(b:Patient) CALL edge2Time.TimeWalkPath(p,"%s","%s",%d,0.85) YIELD path RETURN count(*)
        """
            }
    };

    public static final String [][] nycMirroredQueries = {
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE s1.region_id <> s2.region_id
        RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE s1.region_id <> s2.region_id
        CALL edge2Time.TimeWalkPath(
          p,
          "%s",
          "%s",
          %d,0.85
        ) YIELD path
        RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station) RETURN count(*) AS sel
        """,
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        CALL edge2Time.TimeWalkPath(
          p,
          "%s",
          "%s",
          %d,0.85
        ) YIELD path
        RETURN count(*)
        """
            },
            {
                    """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE s1.region_id <> s2.region_id
        RETURN count(*) AS sel
        """,
        """
        CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]-(s2:Station)
        WHERE s1.region_id <> s2.region_id
        CALL edge2Time.TimeWalkPath(
          p,
          "%s",
          "%s",
          %d,0.85
        ) YIELD path
        RETURN count(*)
        """
            },
            {
        """
        CYPHER RUNTIME=PARALLEL
       MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
       WHERE  s1.capacity>20 and  s2.capacity>20
       RETURN COUNT(*) AS sel
      """,
        """
         CYPHER RUNTIME=PARALLEL
        MATCH p=(s1:Station)-[:SUPER_EDGE]->(s2:Station)
        WHERE  s1.capacity>20 and  s2.capacity>20
        CALL edge2Time.TimeWalkPath(
          p,
          "%s",
          "%s",
          %d,0.85
        ) YIELD path
        RETURN count(*)
        """
            }
    };

    public static void main(String[] args){
        var url = "bolt://localhost:7687";
        final int ITERATION = 3;
        Dotenv dotenv = Dotenv.load();
        var usr = dotenv.get("USERNAME");
        var pwd = dotenv.get("PASSWORD");

        try (var driver = GraphDatabase.driver(url, AuthTokens.basic(usr, pwd))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");
            String delimiter = ";";

            boolean append = true;

            long timestamp = System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(
                            String.format("src/main/resources/selectivity_time.csv_%d", timestamp),
                            append
                    )
            );
            if (!append)
                writer.write("dataset"+delimiter+"queryId"+delimiter+"window"+delimiter+"relation"+delimiter+"ts"+delimiter+"selectivity"+delimiter+"query_time\n");


//            String[] datasetNames = {"finbench", "synthea", "nyc"};
            String [] datasetNames = {"nyc"};
            int [] syntheaWindows = {4, 5, 7};
            int [] finbenchWindows = {30, 50, 70};
            int [] nycWindows = {30, 50, 70};
            String [] relations = {"overlaps", "before", "meets", "equal"};
            String [] syntheaTimeSeries= {"bmi", "bodyWeight", "bodyHeight", "heartRate", "respiratoryRate"};
            String [] finbenchTimeSeries= {"balance"};
            String [] nycTimeSeries = {"num_bikes_available", "num_docks_disabled", "num_bikes_disabled","num_ebikes_available"};

//            String[][][] datasets = {finbenchMirroredQueries, syntheaMirroredQueries, nycMirroredQueries};
            String[][][] datasets = {nycMirroredQueries};
            for ( int d =0; d< datasets.length; d++){
                String datasetName = datasetNames[d];
                String[][] dataset = datasets[d];
                switch (datasetName) {
                    case "finbench" -> {
                        runDataset(relations, finbenchTimeSeries, finbenchWindows, dataset, driver, ITERATION, writer, datasetName, delimiter);
                    }
                    case "synthea" -> {
                        runDataset(relations, syntheaTimeSeries, syntheaWindows, dataset, driver, ITERATION, writer, datasetName, delimiter);
                    }
                    case "nyc" -> {
                        runDataset(relations, nycTimeSeries, nycWindows, dataset, driver, ITERATION, writer, datasetName, delimiter);
                    }
                }
            }


            writer.close();
            System.out.println("Execution times written to file.");

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runDataset(String[] relations, String[] timeSeries, int[] windows, String[][] dataset, Driver driver, int ITERATION, BufferedWriter writer, String datasetName, String delimiter) throws IOException, InterruptedException {
        int queryId = 4;

        ProcessBuilder pb = null;
        if (datasetName.equals("synthea"))
            System.out.println("Please run synthea manually to generate the data.");
//            pb = new ProcessBuilder("sh", "src/main/resources/synthea_10k.sh");
        else if (datasetName.equals("finbench"))
            pb = new ProcessBuilder("sh", "finbench_500k.sh");
        else if (datasetName.equals("nyc"))
            System.out.println("Please run synthea manually to generate the data.");
//            pb = new ProcessBuilder("sh", "src/main/resources/nyc.sh");

//        assert pb != null;
//        pb.redirectErrorStream(true);
//        Process process = pb.start();
//        int exitCode = process.waitFor();
//        if (exitCode != 0) {
//            throw new RuntimeException("finbench_500k.sh failed with exit code " + exitCode);
//        }
        for(String[] query: dataset){
            EagerResult res = driver.executableQuery(query[0]).execute();
            long value = res.records().get(0).get("sel").asLong();
            for (String relation: relations){
                for (String ts: timeSeries){
                    for(int w: windows){
                        long totalTime = 0;
                        for (int i = 0; i < ITERATION; i++) {
                            String newQuery = String.format(query[1], ts,relation,w);
                            res = driver.executableQuery(newQuery).execute();
                            totalTime += res.summary().resultConsumedAfter(TimeUnit.MILLISECONDS);
                        }
                        long avgTime = totalTime / ITERATION;
                        writer.write(datasetName +delimiter+queryId+ delimiter +w+ delimiter+relation+delimiter+ts+delimiter +value + delimiter + avgTime + "\n");
                        writer.flush();

                    }

                }
            }
            queryId++;
        }
    }

}
