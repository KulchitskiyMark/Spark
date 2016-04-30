package vsu.amm;

import com.mongodb.client.MongoCollection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.*;


public class SparkComputing
{
    private JavaSparkContext sc;
    private JavaRDD<String> reviewsFile;
    private JavaRDD<String> filmsFile;
    private JavaRDD<String> genresFile;
    private MongoClient mongoClient;
    private  MongoDatabase db;

    SparkComputing()
    {
        SparkConf conf = new SparkConf()
                .setAppName("Test Spark app")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");
        sc = new JavaSparkContext(conf);
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("Ratings");
        loadFiles();
    }

    public void loadFiles()
    {
        reviewsFile = sc.textFile(".\\src\\main\\resources\\ub.test");
        filmsFile = sc.textFile((".\\src\\main\\resources\\u.item"));
        genresFile = sc.textFile(".\\src\\main\\resources\\u.genre");
    }

    // Get top genres
    public void topGenres()
    {
        // Do pair (film_id : 1)
        JavaPairRDD<Integer, Integer> filmPairs = reviewsFile.mapToPair(
                (String s) ->
                {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]), 1);
                }
        );

        // Summarize the number of reviews for each film
        JavaPairRDD<Integer, Integer> filmCount = filmPairs.reduceByKey(
                (Integer a, Integer b) -> a + b
        );

        //Get genres from file ( title : genre_id)
        JavaPairRDD<Integer, String> genres = genresFile.mapToPair(
                (String s) ->
                {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[1]), line[0]);
                }
        );

        //Get film_id and genres (film_id : array of genres)
        JavaPairRDD<Integer, ArrayList<Integer>> films = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    ArrayList<Integer> list = new ArrayList<>();
                    for (int i = 0; i < 19; i++) {
                        if (Integer.parseInt(line[i + 5]) != 0)
                            list.add(i);
                    }
                    return new Tuple2<>(Integer.parseInt(line[0]), list);
                }
        );

        //Join and get (film_id : (count reviews, array genres))
        JavaPairRDD<Integer, Tuple2<Integer, ArrayList<Integer>>> filmGenres = filmCount.join(films);

        //Get pairs (genre_id : count reviews)
        JavaPairRDD<Integer, Integer> genreCount = filmGenres.values().flatMapToPair(
                (Tuple2<Integer, ArrayList<Integer>> t) -> {
                    ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<>();
                    for (Integer el : t._2()) {
                        list.add(new Tuple2<>(el, t._1()));
                    }
                    return list;
                }
        );

        // Summarize the number of reviews for each genre
        JavaPairRDD<Integer, Integer> countGenres = genreCount.reduceByKey(
                (Integer a, Integer b) -> a + b
        );

        // Join and get title genres
        JavaPairRDD<Integer, Tuple2<String, Integer>> result = genres.join(countGenres);

        // Transform into an ArrayList, sorting and write to the database
        ArrayList<Tuple2<String, Integer>> l = new ArrayList(result.values().collect());
        Collections.sort(l, (o1, o2) -> o2._2().compareTo(o1._2()));

        MongoCollection<Document> collection = db.getCollection("TopGenres");
        Document doc = new Document();

        for (Tuple2<String, Integer> t : l) {
            doc.append(t._1(), t._2());
        }
        collection.insertOne(doc);
    }
}