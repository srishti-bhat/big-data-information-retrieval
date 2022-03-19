package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class NewsArticleQueriesMap implements Serializable {

    private static final long serialVersionUID = 1565312130567946299L;

    NewsArticle newsArticle; 
    Map<String, Short> queryTermFrequencyMap;

    public NewsArticleQueriesMap(NewsArticle newsArticle, Map<String, Short> queryTermFrequencyMap) {
        super();
        this.newsArticle = newsArticle;
        this.queryTermFrequencyMap = queryTermFrequencyMap;
    }


    public NewsArticleQueriesMap() {
    }


    public NewsArticle getNewsArticle() {
        return newsArticle;
    }


    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }


    public Map<String, Short> getQueryTermFrequencyMap() {
        return queryTermFrequencyMap;
    }


    public void setQueryTermFrequencyMap(Map<String, Short> queryTermFrequencyMap) {
        this.queryTermFrequencyMap = queryTermFrequencyMap;
    }



}
