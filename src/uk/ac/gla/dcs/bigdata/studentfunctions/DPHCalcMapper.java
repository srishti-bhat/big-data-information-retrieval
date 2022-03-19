package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;

public class DPHCalcMapper implements MapFunction<Query,DocumentRanking>{

    Broadcast<List<Query>> queryBroadcast;
    Broadcast<List<NewsArticleQueriesMap>> newsArticlesQueriesMapListBroadcast;
    Broadcast<Long> totalDocsInCorpusAccBroadcast;
	Broadcast<Double> averageDocumentLengthInCorpusBroadcast;
	Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast;
    LongAccumulator termFrequencyInCorpus;
	LongAccumulator currDocumentLength;
    DoubleAccumulator avgScoreAcc;

    private transient TextPreProcessor processor;


    public DPHCalcMapper(Broadcast<List<Query>> queryBroadcast,
            Broadcast<List<NewsArticleQueriesMap>> newsArticlesQueriesMapListBroadcast,
            Broadcast<Long> totalDocsInCorpusAccBroadcast, Broadcast<Double> averageDocumentLengthInCorpusBroadcast,
            Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast,
            LongAccumulator termFrequencyInCorpus, LongAccumulator currDocumentLength, DoubleAccumulator avgScoreAcc) {
        this.queryBroadcast = queryBroadcast;
        this.newsArticlesQueriesMapListBroadcast = newsArticlesQueriesMapListBroadcast;
        this.totalDocsInCorpusAccBroadcast = totalDocsInCorpusAccBroadcast;
        this.averageDocumentLengthInCorpusBroadcast = averageDocumentLengthInCorpusBroadcast;
        this.corpusTermFrequencyFlattenedListBroadcast = corpusTermFrequencyFlattenedListBroadcast;
        this.termFrequencyInCorpus = termFrequencyInCorpus;
        this.currDocumentLength = currDocumentLength;
        this.avgScoreAcc = avgScoreAcc;
    }

    @Override
    public DocumentRanking call(Query query) throws Exception {
        
        if (processor==null) processor = new TextPreProcessor();

        List<RankedResult> rankedResults = new ArrayList<RankedResult>();
        List<DocumentRanking> documentRankings = new ArrayList<DocumentRanking>();

        List<Tuple2<String, Integer>> corpusTermFrequencyFlattenedListBroadcastList = corpusTermFrequencyFlattenedListBroadcast.value();
        
        newsArticlesQueriesMapListBroadcast.value().forEach(newsArticleQueryMap->{
            avgScoreAcc.setValue(0.0);
            query.getQueryTerms().forEach(term->{
                double score = 0.0;
                corpusTermFrequencyFlattenedListBroadcastList.forEach(corpusTerm -> {
                    if(term.compareTo(corpusTerm._1) == 0) {
                        termFrequencyInCorpus.setValue(corpusTerm._2.intValue());
                    }
                });
            
                    
                currDocumentLength.setValue(0);
                newsArticleQueryMap.getNewsArticle().getContents().forEach(content -> {
                    if(content.getContent() != null){
                        currDocumentLength.add(terms2String(processor.process(content.getContent())).length());
                    }
                });
                short v1 = (short)0;
                for (Map.Entry entry: newsArticleQueryMap.getQueryTermFrequencyMap().entrySet())
                    v1 = (short)entry.getValue();
        
                int v2 = (int)termFrequencyInCorpus.sum();
                int v3 = (int)currDocumentLength.sum();
                Double v4 = averageDocumentLengthInCorpusBroadcast.value();
                long v5 = totalDocsInCorpusAccBroadcast.value().longValue();
                
                score = DPHScorer.getDPHScore(
                    v1, 
                    (int)termFrequencyInCorpus.sum(),
                    (int)currDocumentLength.sum(), 
                    averageDocumentLengthInCorpusBroadcast.value(), 
                    totalDocsInCorpusAccBroadcast.value().longValue()
                );

                if (Double.isNaN(score) || Double.isInfinite(score))
					score = 0.0;
				
				avgScoreAcc.add(score);
                });
                int qsize = query.getQueryTerms().size();
                double finalScore = avgScoreAcc.sum()/qsize;
                rankedResults.add(
                        new RankedResult(newsArticleQueryMap.getNewsArticle().getId(), 
                        newsArticleQueryMap.getNewsArticle(),
                        finalScore));
                
            });
            
        return new DocumentRanking(
            query,
            rankedResults
        );
    }

    /**
	 * Utility method that converts a List<String> to a string
	 * @param terms
	 * @return
	 */
	public String terms2String(List<String> terms) {
		StringBuilder builder = new StringBuilder();
		for (String term : terms) {
			builder.append(term);
			builder.append(" ");
		}
		return builder.toString();
	}
    
}
