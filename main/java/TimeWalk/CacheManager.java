package TimeWalk;

import org.mapdb.*;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class CacheManager {

    private static DB db;

    // ✅ GLOBAL CACHE NOW STORES FULL TD-JOIN RESULTS
    private static ConcurrentMap<String, CachedResult> cache;

    public static synchronized void initDB() {
        if (db != null && !db.isClosed()) return;

        db = DBMaker.memoryDB().make();

        cache = db.hashMap("tdjoin_cache")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA) // IMPORTANT
                .createOrOpen();
    }

    public static synchronized ConcurrentMap<String, CachedResult> getCache() {
        initDB();
        return cache;
    }

    public static synchronized void clearCache(Log log) {
        try {
            if (cache != null) cache.clear();
            if (db != null && !db.isClosed()) db.close();

            cache = null;
            db = null;

            if (log != null) log.info("TD-JOIN cache cleared.");
        } catch (Exception e) {
            if (log != null) log.error("Cache clear failed: " + e.getMessage(), e);
        }
    }

    // =====================================================
    // GLOBAL CACHE ENTRY (NOW CENTRALIZED HERE)
    // =====================================================
    public static class CachedResult implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        public Map<String, List<double[]>> result;
        public boolean match;
        public int seqAIndex;
        public int seqBIndex;

        public CachedResult() {}

        public CachedResult(Map<String, List<double[]>> result,
                            boolean match,
                            int seqAIndex,
                            int seqBIndex) {
            this.result = result;
            this.match = match;
            this.seqAIndex = seqAIndex;
            this.seqBIndex = seqBIndex;
        }
    }
}