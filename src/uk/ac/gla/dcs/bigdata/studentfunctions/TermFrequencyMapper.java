package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

public class TermFrequencyMapper implements FlatMapFunction<NewsArticleQueriesMap, TermFrequency>{

    @Override
    public Iterator<TermFrequency> call(NewsArticleQueriesMap newsArticleQueriesMap) throws Exception {
        
        List<TermFrequency> termFrequencyList = new ArrayList<TermFrequency>();
        
        for (Map.Entry entry: newsArticleQueriesMap.getQueryTermFrequencyMap().entrySet())
            termFrequencyList.add(
                new TermFrequency(
                    entry.getKey().toString(), (Long)entry.getValue()
                    )
                );
       
        return termFrequencyList.iterator();
    }
    
}
