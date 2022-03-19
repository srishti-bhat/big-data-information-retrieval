package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;

public class DPHCalcMapper implements FlatMapFunction<Query,DocumentRanking>{

    Broadcast<List<Query>> queryBroadcast;
    Broadcast<List<NewsArticleQueriesMap>> newsArticlesQueriesMapListBroadcast;
    Broadcast<Long> totalDocsInCorpusAccBroadcast;
	Broadcast<Double> averageDocumentLengthInCorpusBroadcast;
	Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast;
    LongAccumulator termFrequencyInCorpus;
	LongAccumulator currDocumentLength;

    private transient TextPreProcessor processor;


    public DPHCalcMapper(Broadcast<List<Query>> queryBroadcast,
            Broadcast<List<NewsArticleQueriesMap>> newsArticlesQueriesMapListBroadcast,
            Broadcast<Long> totalDocsInCorpusAccBroadcast, Broadcast<Double> averageDocumentLengthInCorpusBroadcast,
            Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast,
            LongAccumulator termFrequencyInCorpus, LongAccumulator currDocumentLength) {
        this.queryBroadcast = queryBroadcast;
        this.newsArticlesQueriesMapListBroadcast = newsArticlesQueriesMapListBroadcast;
        this.totalDocsInCorpusAccBroadcast = totalDocsInCorpusAccBroadcast;
        this.averageDocumentLengthInCorpusBroadcast = averageDocumentLengthInCorpusBroadcast;
        this.corpusTermFrequencyFlattenedListBroadcast = corpusTermFrequencyFlattenedListBroadcast;
        this.termFrequencyInCorpus = termFrequencyInCorpus;
        this.currDocumentLength = currDocumentLength;
    }

    @Override
    public Iterator<DocumentRanking> call(Query query) throws Exception {
        
        if (processor==null) processor = new TextPreProcessor();

        List<RankedResult> rankedResults = new ArrayList<RankedResult>();
        List<DocumentRanking> documentRankings = new ArrayList<DocumentRanking>();

        List<Tuple2<String, Integer>> corpusTermFrequencyFlattenedListBroadcastList = corpusTermFrequencyFlattenedListBroadcast.value();
        
            query.getQueryTerms().forEach(term->{
                corpusTermFrequencyFlattenedListBroadcastList.forEach(corpusTerm -> {
                    if(term.compareTo(corpusTerm._1) == 0) {
                        termFrequencyInCorpus.setValue(corpusTerm._2.intValue());
                    }
                });
            
                newsArticlesQueriesMapListBroadcast.value().forEach(newsArticleQueryMap->{
                    
                    currDocumentLength.setValue(0);
                    newsArticleQueryMap.getNewsArticle().getContents().forEach(content -> {
                        if(content.getContent() != null){
                            currDocumentLength.add(terms2String(processor.process(content.getContent())).length());
                        }
                    });
                    if(newsArticleQueryMap.getQueryTermFrequencyMap().get(term).shortValue() != 0){
                        double score = DPHScorer.getDPHScore(
                            newsArticleQueryMap.getQueryTermFrequencyMap().get(term), 
                            (int)termFrequencyInCorpus.sum(),
                            (int)currDocumentLength.sum(), 
                            averageDocumentLengthInCorpusBroadcast.value(), 
                            totalDocsInCorpusAccBroadcast.value().longValue()
                        );
                        rankedResults.add(
                            new RankedResult(newsArticleQueryMap.getNewsArticle().getId(), 
                            newsArticleQueryMap.getNewsArticle(),
                            score));
                    }
                });
            });

            documentRankings.add(new DocumentRanking(
                query,
                rankedResults
            ));


        return documentRankings.iterator();
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
