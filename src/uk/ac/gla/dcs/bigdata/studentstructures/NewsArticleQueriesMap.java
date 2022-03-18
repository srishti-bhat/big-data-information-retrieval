package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class NewsArticleQueriesMap implements Serializable {

    private static final long serialVersionUID = 1565312130567946299L;

    NewsArticle newsArticle; 
	List<Query> queryList; 
    Map<String, Long> queryTermFrequencyMap;

    public NewsArticleQueriesMap(NewsArticle newsArticle, List<Query> queryList,
            Map<String, Long> queryTermFrequencyMap) {
        this.newsArticle = newsArticle;
        this.queryList = queryList;
        this.queryTermFrequencyMap = queryTermFrequencyMap;
    }


    public NewsArticleQueriesMap() {
    }

    
    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public List<Query> getQueryList() {
        return queryList;
    }

    public void setQueryList(List<Query> queryList) {
        this.queryList = queryList;
    }


    public Map<String, Long> getQueryTermFrequencyMap() {
        return queryTermFrequencyMap;
    }


    public void setQueryTermFrequencyMap(Map<String, Long> queryTermFrequencyMap) {
        this.queryTermFrequencyMap = queryTermFrequencyMap;
    }


}
