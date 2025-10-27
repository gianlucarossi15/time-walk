package edge2Time;

import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IntermediateResult {

    Map<String, List<List<Double>>> joinResult;
    public IntermediateResult(Map<String, List<List<Double>>> joinResult) {
        this.joinResult = joinResult;
    }
    public IntermediateResult() {
        this.joinResult = null; // No TD-Join result
    }

    public Map<String, List<List<Double>>> getJoinResult() {
        if (this.joinResult != null)
            return joinResult;
        return null;
    }
}
