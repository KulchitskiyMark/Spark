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

import java.io.Serializable;
import java.util.*;


public class SparkComputing
{
    private JavaSparkContext sc;
    private JavaRDD<String> reviewsFile;
    private JavaRDD<String> filmsFile;
    private JavaRDD<String> genresFile;
    private JavaRDD<String> usersFile;
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
        usersFile = sc.textFile(".\\src\\main\\resources\\u.user");
    }

    ///////////////////////////////////////
    // Get top genres in the number of reviews to the films
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

        List<String> genres = genresFile.map(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return line[0];
                }
        ).collect();

        // Get pairs film_id : genres
        JavaPairRDD<Integer, String> filmGenre = filmsFile.mapToPair(
                 (String s) -> {
                     String[] line = s.split("[|]");
                     StringBuilder genre = new StringBuilder();
                     for (int i = 0; i < 19; i++) {
                         if (Integer.parseInt(line[i + 5]) != 0)
                             genre.append(genres.get(i) + " ");
                     }
                     return new Tuple2<>(Integer.parseInt(line[0]), genre.toString());
                 }
        );

        // Get pairs genres : count
        JavaPairRDD<String,Integer> genreCount = filmCount.join(filmGenre).mapToPair(
                (Tuple2<Integer, Tuple2<Integer, String>> t) -> {
                    return new Tuple2<>(t._2()._2(), t._2()._1());
                }
        );

        // Summarize the reviews and sort
        ArrayList<Tuple2<String, Integer>> result = new ArrayList(genreCount.reduceByKey(
                (Integer a, Integer b) -> a + b).collect());

        Collections.sort(result, (o1, o2) -> o2._2().compareTo(o1._2()));

        MongoCollection<Document> collection = db.getCollection("TopGenres");
        Document doc = new Document();

        for (Tuple2<String, Integer> t : result)
        {
            doc.append(t._1(), t._2());
        }
        collection.insertOne(doc);
    }

    ///////////////////////////////////////
    // Get top-rated movies by release year and genres
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
            doc.append(t._1().toString(), t._2()._2() + " " + t._2()._1());
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

    ///////////////////////////////////////
    // Get top-rated movies (for men, for women, by gender activities)
    public  void topRatedFilmsGender()
    {
        // Get pairs user_id : gander
        JavaPairRDD<Integer, String> users = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[2]);
                }
        );

        // Do pairs film_id : (rating, user_id)
        JavaPairRDD<Integer, Tuple2<Integer,Integer>> filmRated = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]),new Tuple2<>(Integer.parseInt(line[2]), Integer.parseInt(line[0])));
                }
        );

        //Summarize the number of rating for each film
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> sumRate = filmRated.reduceByKey(
                (Tuple2<Integer,Integer> t1, Tuple2<Integer,Integer> t2) -> {
                    return new Tuple2<>(t1._1() + t2._1(), t1._2());
                }
        );

        // Get pairs film_id : title
        JavaPairRDD<Integer,String> filmTitle = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    try {
                        return new Tuple2<>(Integer.parseInt(line[0]), line[1].substring(0,line[1].lastIndexOf("(")));
                    }
                    catch (Exception e){
                        return new Tuple2<>(Integer.parseInt(line[0]), "Unknown");
                    }
                }
        );

        // Join and get pairs user-id : (rate, title)
        JavaPairRDD<Integer, Tuple2<Integer, String>> filmUser = filmTitle.join(sumRate).mapToPair(
                (Tuple2<Integer, Tuple2<String, Tuple2<Integer,Integer>>> t) -> {
                    return new Tuple2<>(t._2()._2()._2(), new Tuple2<>(t._2()._2()._1(), t._2()._1()));
                }
        );

        // Join and get pairs gender : (rate, title)
        JavaPairRDD<String, Tuple2<Integer,String>> userRate = users.join(filmUser).mapToPair(
                (Tuple2<Integer, Tuple2<String, Tuple2<Integer, String>>> t) -> {
                    return new Tuple2<>(t._2()._1(), new Tuple2<>(t._2()._2()._1(), t._2()._2()._2()));
                }
        );

        // Get top-rated films for male
        List<Tuple2<String, Tuple2<Integer, String>>> resultMale = userRate.filter(
                (Tuple2<String, Tuple2<Integer, String>> t) -> {
                    return (t._1().contains("M"));
                }
        ).takeOrdered(10, new rateComparatop());

        MongoCollection<Document> collectionMale = db.getCollection("TopRatedFilmForMale");
        Document docMale = new Document();

        for (Tuple2<String, Tuple2<Integer, String>> t : resultMale)
        {
            docMale.append(t._2()._1().toString(), t._2()._2());
        }
        collectionMale.insertOne(docMale);

        // Get top-rated films for female
        List<Tuple2<String, Tuple2<Integer, String>>> resultFemale = userRate.filter(
                (Tuple2<String, Tuple2<Integer, String>> t) -> {
                    return (t._1().contains("F"));
                }
        ).takeOrdered(10, new rateComparatop());

        MongoCollection<Document> collectionFemale = db.getCollection("TopRatedFilmForFemale");
        Document docFemale = new Document();

        for (Tuple2<String, Tuple2<Integer, String>> t : resultFemale)
        {
            docFemale.append(t._2()._1().toString(), t._2()._2());
        }
        collectionFemale.insertOne(docFemale);

        //////////////////////////////////
        //Get top-rated for occupation

        // Get pairs user_id : occupation
        JavaPairRDD<Integer,String> userOcc = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[3]);
                }
        );

        // Join and get occupation : (rate, title)
        JavaPairRDD<String, Tuple2<Integer,String>> rateOcc = userOcc.join(filmUser).mapToPair(
                (Tuple2<Integer, Tuple2<String, Tuple2<Integer, String>>> t) -> {
                    return new Tuple2<>(t._2()._1(), new Tuple2<>(t._2()._2()._1(), t._2()._2()._2()));
                }
        );

        // Sort by occupation
        JavaPairRDD<String, Tuple2<Integer,String>> resultOcc = rateOcc.reduceByKey(
                (Tuple2<Integer,String> t1, Tuple2<Integer,String> t2) -> {
                    if (t1._1() > t2._1()) return t1;
                    else return t2;
                }
        );

        List<Tuple2<String, Tuple2<Integer,String>>> l = resultOcc.collect();

        MongoCollection<Document> collectionOcc = db.getCollection("TopRatedFilmForOcc");
        Document docOcc = new Document();

        for (Tuple2<String, Tuple2<Integer, String>> t : l)
        {
            docOcc.append(t._1(), t._2()._2() + " " + t._2()._1());
        }
        collectionOcc.insertOne(docOcc);
    }


    // Comparator for (year, genres) : (rate, title)
    public static class yearComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
            return o2._1().compareTo(o1._1());
        }
    }

    // Comparator for gender : (rate, title)
    public static class rateComparatop implements Comparator<Tuple2<String, Tuple2<Integer, String>>>, Serializable {
        @Override
        public int compare(Tuple2<String, Tuple2<Integer, String>> o1, Tuple2<String, Tuple2<Integer, String>> o2) {
            return o2._2()._1().compareTo(o1._2()._1());
        }
    }
}