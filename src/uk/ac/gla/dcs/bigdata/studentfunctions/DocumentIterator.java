package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class DocumentIterator implements FlatMapFunction<NewsArticle, NewsArticle>{

	LongAccumulator totalDocsInCorpusAcc;

	public DocumentIterator(LongAccumulator totalDocsInCorpusAcc) {
        super();
		this.totalDocsInCorpusAcc = totalDocsInCorpusAcc;
	}
	
    @Override
    public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {
        totalDocsInCorpusAcc.add(1);
        List<NewsArticle> newsArticles = new ArrayList<NewsArticle>(1);
        newsArticles.add(newsArticle);

        return newsArticles.iterator();
    }

    
}
