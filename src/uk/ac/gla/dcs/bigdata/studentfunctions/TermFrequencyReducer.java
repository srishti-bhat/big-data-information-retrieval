package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Map;

import org.apache.spark.api.java.function.ReduceFunction;

public class TermFrequencyReducer implements ReduceFunction<Map<String,Integer>> {

    @Override
    public Map<String, Integer> call(Map<String, Integer> arg0, Map<String, Integer> arg1) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    
}
