package com.dawud;

import org.apache.spark.AccumulatorParam;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MapCountAccumulator implements AccumulatorParam<Map<String, Double>>, Serializable {

    @Override
    public Map<String, Double> addAccumulator(Map<String, Double> t1, Map<String, Double> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<String, Double> addInPlace(Map<String, Double> r1, Map<String, Double> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<String, Double> zero(final Map<String, Double> initialValue) {
        return new HashMap<>();
    }

    private Map<String, Double> mergeMap(Map<String, Double> map1, Map<String, Double> map2) {
        Map<String, Double> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> a + b));
        return result;
    }

}
