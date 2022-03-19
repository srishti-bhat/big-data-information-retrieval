package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;

public class DPHCalcMapper implements MapFunction<NewsArticleQueriesMap,DocumentRanking>{

    Broadcast<Long> totalDocsInCorpusAccBroadcast;
	Broadcast<Double> averageDocumentLengthInCorpusBroadcast;
	Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast;

    
    @Override
    public DocumentRanking call(NewsArticleQueriesMap newsArticleQueriesMap) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    
}
