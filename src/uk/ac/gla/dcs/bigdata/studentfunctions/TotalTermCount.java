package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

public class TotalTermCount implements MapGroupsFunction<String, TermFrequency, Tuple2<String,Integer>>{

    @Override
    public Tuple2<String, Integer> call(String key, Iterator<TermFrequency> termFrequencies) throws Exception {

		int count = 0;
		while (termFrequencies.hasNext()) {
			TermFrequency termFrequency = termFrequencies.next();
			count += termFrequency.getTotalCount();
		}
	    return new Tuple2<String,Integer>(key,count);
    }
    
}
