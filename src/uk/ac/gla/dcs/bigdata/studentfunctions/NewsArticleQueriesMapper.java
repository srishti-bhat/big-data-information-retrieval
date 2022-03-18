package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;

public class NewsArticleQueriesMapper implements MapFunction<NewsArticle, NewsArticleQueriesMap>{
    
    Broadcast<List<Query>> queryBroadcast;

    @Override
    public NewsArticleQueriesMap call(NewsArticle arg0) throws Exception {
        
        return null;
    }

}
