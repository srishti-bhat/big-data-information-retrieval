package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class DocumentIterator implements FlatMapFunction<NewsArticle, NewsArticle>{

	LongAccumulator totalDocsInCorpusAcc;
    LongAccumulator totalDocumentLengthInCorpusAcc;
	
    private transient TextPreProcessor processor;

	public DocumentIterator(LongAccumulator totalDocsInCorpusAcc, LongAccumulator totalDocumentLengthInCorpusAcc) {
        super();
		this.totalDocsInCorpusAcc = totalDocsInCorpusAcc;
        this.totalDocumentLengthInCorpusAcc = totalDocumentLengthInCorpusAcc;
	}
	
    @Override
    public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {

        if (processor==null) processor = new TextPreProcessor();

        newsArticle.getContents().forEach(content -> {
            if (content.getContent()!=null) {
				String processedContent = terms2String(processor.process(content.getContent()));
				if(processedContent != null){
                    totalDocumentLengthInCorpusAcc.add(processedContent.length());
                }
			}
        });
        
        totalDocsInCorpusAcc.add(1);

        List<NewsArticle> newsArticles = new ArrayList<NewsArticle>(1);

        newsArticles.add(newsArticle);

        return newsArticles.iterator();
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
