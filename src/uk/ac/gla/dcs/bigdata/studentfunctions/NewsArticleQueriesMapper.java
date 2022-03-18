package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;

public class NewsArticleQueriesMapper implements MapFunction<NewsArticle, NewsArticleQueriesMap>{
    
    Broadcast<List<Query>> queryBroadcast;
    private transient TextPreProcessor processor;
    LongAccumulator termFrequencyInCurrentDocumentAcc;

    public NewsArticleQueriesMapper(Broadcast<List<Query>> queryBroadcast,  LongAccumulator termFrequencyInCurrentDocumentAcc) {
        super();
		this.queryBroadcast = queryBroadcast;
        this.termFrequencyInCurrentDocumentAcc = termFrequencyInCurrentDocumentAcc;
	}

    @Override
    public NewsArticleQueriesMap call(NewsArticle newsArticle) throws Exception {

        NewsArticle newsArt = newsArticle;
        if (processor==null) processor = new TextPreProcessor();

        List<Query> queryBroadcastList = queryBroadcast.value();
        
        /*newsArticle.getContents().forEach(content-> {
            String processedContent = terms2String(processor.process(content.getContent()));
        }); */
        List<String> queryTermsFlattened = new ArrayList<String>();
        Map<String,Long> termFreq = new HashMap<String,Long>();

        queryBroadcastList.forEach(query -> {
            query.getQueryTerms().forEach(queryTerm -> {
                queryTermsFlattened.add(queryTerm);
            });
        });

        queryTermsFlattened.forEach(term-> {
            termFrequencyInCurrentDocumentAcc.setValue(0);
            newsArticle.getContents().forEach(content->{
                if(content.getContent() != null){
                    if(content.getContent().contains(term)){
                        termFrequencyInCurrentDocumentAcc.add(1);
                    }
                }
            });
            termFreq.put(term, termFrequencyInCurrentDocumentAcc.value());
        });

        return new NewsArticleQueriesMap(newsArt, queryBroadcastList, termFreq);
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
