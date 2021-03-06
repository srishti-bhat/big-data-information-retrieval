package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.DoubleAccumulator;


import scala.Tuple2;
import java.util.Collections;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHCalcMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentIterator;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleQueriesMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.RedundancyCheck;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermFrequencyKeyFunction;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermFrequencyMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.TestTokenize;
import uk.ac.gla.dcs.bigdata.studentfunctions.TotalTermCount;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleQueriesMap;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("BIGDATA_QUERIES");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("BIGDATA_NEWS");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		System.out.println(results.size());

		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(new File(resultsDIR).getAbsolutePath());
			}
		}
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		newsjson = newsjson.repartition(24);

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// try a collect as list
		List<Query> queryList = queries.collectAsList();
		
		LongAccumulator totalDocumentLengthInCorpusAcc = spark.sparkContext().longAccumulator();
		LongAccumulator totalDocsInCorpusAcc = spark.sparkContext().longAccumulator();
		LongAccumulator termFrequencyInCurrentDocumentAcc = spark.sparkContext().longAccumulator();
		LongAccumulator termFrequencyInCorpus = spark.sparkContext().longAccumulator();
		LongAccumulator currDocumentLength = spark.sparkContext().longAccumulator();
		DoubleAccumulator avgScoreAcc = spark.sparkContext().doubleAccumulator();


		// try a map function
		Dataset<NewsArticle> newsTokenized = news.map(new TestTokenize(), Encoders.bean(NewsArticle.class));
		Dataset<NewsArticle> newsArticles = newsTokenized.flatMap(new DocumentIterator(totalDocsInCorpusAcc, totalDocumentLengthInCorpusAcc), Encoders.bean(NewsArticle.class));
		List<NewsArticle> newsArticlesList = newsArticles.collectAsList();

		Double averageDocumentLengthInCorpus = totalDocumentLengthInCorpusAcc.value() / (1.0 * totalDocsInCorpusAcc.value());

		Broadcast<List<Query>> queryBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
		
		Dataset<NewsArticleQueriesMap> newsArticlesQueriesMap = newsTokenized.map(new NewsArticleQueriesMapper(queryBroadcast, termFrequencyInCurrentDocumentAcc), Encoders.bean(NewsArticleQueriesMap.class));
		List<NewsArticleQueriesMap> newsArticlesQueriesMapList = newsArticlesQueriesMap.collectAsList();
		Broadcast<List<NewsArticleQueriesMap>> newsArticlesQueriesMapListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(newsArticlesQueriesMapList);

		Dataset<TermFrequency> corpusTermFrequency = newsArticlesQueriesMap.flatMap(new TermFrequencyMapper(), Encoders.bean(TermFrequency.class));
		KeyValueGroupedDataset<String, TermFrequency> corpusTermFrequencyGrouped = corpusTermFrequency.groupByKey(new TermFrequencyKeyFunction(), Encoders.STRING());		
		Encoder<Tuple2<String,Integer>> termEncoder = Encoders.tuple(Encoders.STRING(), Encoders.INT());
		Dataset<Tuple2<String, Integer>> corpusTermFrequencyFlattened = corpusTermFrequencyGrouped.mapGroups(new TotalTermCount(), termEncoder);
		List<Tuple2<String, Integer>> corpusTermFrequencyFlattenedList = corpusTermFrequencyFlattened.collectAsList();

		Broadcast<Long> totalDocsInCorpusAccBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpusAcc.value());
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		Broadcast<List<Tuple2<String, Integer>>> corpusTermFrequencyFlattenedListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(corpusTermFrequencyFlattenedList);

		Dataset<DocumentRanking> documentRanking = queries.map(
			new DPHCalcMapper(
				queryBroadcast, 
				newsArticlesQueriesMapListBroadcast,
				totalDocsInCorpusAccBroadcast, 
				averageDocumentLengthInCorpusBroadcast, 
				corpusTermFrequencyFlattenedListBroadcast,
				termFrequencyInCorpus,
				currDocumentLength,
				avgScoreAcc
				), 
			Encoders.bean(DocumentRanking.class));
		
		Dataset<DocumentRanking> documentRankingFinal = documentRanking.map(new RedundancyCheck(), Encoders.bean(DocumentRanking.class));
		
		List<DocumentRanking> documentRankingList = documentRankingFinal.collectAsList();
		return documentRankingList; 
	}
	
	
}
