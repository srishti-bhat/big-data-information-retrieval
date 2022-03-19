package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

public class TermFrequencyKeyFunction implements MapFunction<TermFrequency,String> {

    @Override
    public String call(TermFrequency termFrequency) throws Exception {
        return termFrequency.getTerm();
    }
    
}
