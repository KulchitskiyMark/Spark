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
import scala.Tuple3;
import scala.util.parsing.combinator.testing.Str;

import java.io.Serializable;
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

    private void loadFiles()
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

    ///////////////////////////////////////
    // Get top-rated movies by release year
    public void topRatedFilms()
    {
        // Do pairs film_id : rating
        JavaPairRDD<Integer, Integer> filmRated = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]),Integer.parseInt(line[2]));
                }
        );

        //Summarize the number of rating for each film
        JavaPairRDD<Integer,Integer> sumRate = filmRated.reduceByKey(
                (Integer a, Integer b) -> a+b
        );

        // Do pairs film_id : <year, title>
        JavaPairRDD<Integer, Tuple2<Integer, String>> filmYear = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    try {
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>( Integer.parseInt(line[2].substring(7)),line[1].substring(0,line[1].lastIndexOf("("))));
                    }
                    catch (Exception e){
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>(0, "Unknown"));
                    }
                }
        );

        // Get pairs year : <rating, title>
        JavaPairRDD<Integer, Tuple2<Integer, String>> filmRankYear = filmYear.join(sumRate).values().mapToPair(
                (Tuple2<Tuple2<Integer,String>, Integer> t) -> new Tuple2<>(t._1()._1(), new Tuple2<>(t._2(),t._1()._2()))
        );

        // Get pair with more rating
        JavaPairRDD<Integer, Tuple2<Integer, String>> result = filmRankYear.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1() > t2._1()) return t1;
                    else return t2;
                }
        );

        List<Tuple2<Integer, Tuple2<Integer, String>>> l = result.sortByKey(false).collect();

        MongoCollection<Document> collection = db.getCollection("TopRatedFilmForYearsAll");
        Document doc = new Document();

        for (Tuple2<Integer, Tuple2<Integer, String>> t : l)
        {
            doc.append(t._1().toString(), t._2()._2());
        }
        collection.insertOne(doc);

        ///////////////////////////////////////////////
        // Top-rated films for year and genres

        List<String> genres = genresFile.map(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return line[0];
                }
        ).collect();

        // Do pairs film_id : (year, title, genres)
        JavaPairRDD<Integer, Tuple3<Integer, String, String>> filmYearGenre = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    StringBuilder genre = new StringBuilder();
                    for (int i = 0; i < 19; i++) {
                        if (Integer.parseInt(line[i + 5]) != 0)
                            genre.append(genres.get(i) + " ");
                    }
                    try {
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple3<>(Integer.parseInt(line[2].substring(7)), line[1].substring(0, line[1].lastIndexOf("(")), genre.toString()));
                    }
                    catch (Exception e){
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple3<>(0, "Unknown", "unknown"));
                    }
                }
        );

        // Join and get paris film_id : (rate , year , title, genres)
        JavaPairRDD<Integer , Tuple2<Integer, Tuple3<Integer, String, String>>> joinFilmRate = sumRate.join(filmYearGenre);

        // Do pairs (year, genres) : (rate, title)
        JavaPairRDD<Tuple2<Integer,String>,Tuple2<Integer,String>> YearGenre = joinFilmRate.values().mapToPair(
                (Tuple2<Integer, Tuple3<Integer, String, String>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._2()._1(), t._2()._3()), new Tuple2<>(t._1(), t._2()._2()));
                }
        );

        // Get top-rated film for years and genres
        JavaPairRDD<Tuple2<Integer,String>,Tuple2<Integer,String>> result2 = YearGenre.reduceByKey(
                (Tuple2<Integer,String> t1, Tuple2<Integer,String> t2) -> {
                    if (t1._1() > t2._1()) return t1;
                    else return t2;
                }
        );

        List<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>> l2 = result2.sortByKey(new yearComparator()).collect();

        MongoCollection<Document> collection2 = db.getCollection("TopRatedFilmForYearsGenre");
        Document doc2 = new Document();

        for (Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>> t : l2)
        {
            doc2.append(t._1()._1() + " " + t._1()._2(), t._2()._2() + " " + t._2()._1());
        }
        collection2.insertOne(doc2);
    }


    // Comparator for (year, genres) : (rank, title)
    public static class yearComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
            return o2._1().compareTo(o1._1());
        }
    }
}