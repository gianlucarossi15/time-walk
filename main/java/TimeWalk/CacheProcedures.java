package TimeWalk;

import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

public class CacheProcedures {

    @Context
    public Log log;

    public static class ClearCacheOutput {
        public String message;

        public ClearCacheOutput(String message) {
            this.message = message;
        }
    }

    @Procedure(name = "edge2Time.clearCache", mode = Mode.READ)
    public Stream<ClearCacheOutput> clearCache() {
        CacheManager.clearCache(log);
        return Stream.of(new ClearCacheOutput("Cache cleared"));
    }
}