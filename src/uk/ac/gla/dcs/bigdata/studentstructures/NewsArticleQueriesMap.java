package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class NewsArticleQueriesMap implements Serializable {

    private static final long serialVersionUID = 1565312130567946299L;

    NewsArticle newsArticle; 
	List<Query> queryList; 
    
}
