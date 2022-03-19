package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

public class RedundancyCheck implements MapFunction<DocumentRanking,DocumentRanking> {

    @Override
    public DocumentRanking call(DocumentRanking arg0) throws Exception {

        return null;
    }
    
}
