package vsu.amm;

import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import scala.Tuple2;
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

    SparkComputing()
    {
        SparkConf conf = new SparkConf()
                .setAppName("Test Spark app")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");
        sc = new JavaSparkContext(conf);
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
                            genre.append(genres.get(i)).append(" ");
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

        // Summarize the reviews for each genre
        JavaPairRDD<String, Integer> result = genreCount.reduceByKey(
                (Integer a, Integer b) -> a + b);

        JavaPairRDD<Integer, String> resultSorted = result.mapToPair(
                (Tuple2<String, Integer> t) -> {
                    return new Tuple2<>(t._2(),t._1());
                }
        ).sortByKey(false);

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopGenres");

        JavaPairRDD<Object, BSONObject> save = resultSorted.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("genres", t._2());
                    bson.put("reviews", t._1());
                    return new Tuple2<>(null, bson);
                }
        );
        save.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    ///////////////////////////////////////
    // Get top-rated films by release year and genres
    public void topRatedFilms()
    {
        // Top-rated films by release year among all

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

        // Get pairs year : <rate, title>
        JavaPairRDD<Integer, Tuple2<Integer, String>> filmRankYear = filmYear.join(sumRate).values().mapToPair(
                (Tuple2<Tuple2<Integer,String>, Integer> t) -> new Tuple2<>(t._1()._1(), new Tuple2<>(t._2(),t._1()._2()))
        );

        // Get pair with more rate
        JavaPairRDD<Integer, Tuple2<Integer, String>> result = filmRankYear.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1() > t2._1()) return t1;
                    else return t2;
                }
        ).sortByKey(false);

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedByYearAll");

        JavaPairRDD<Object, BSONObject> save = result.mapToPair(
                (Tuple2<Integer,Tuple2 <Integer, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("year", t._1());
                    bson.put("rate", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        save.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

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
                            genre.append(genres.get(i)).append(" ");
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

        // Do pairs year,rate : genre, title
        JavaPairRDD<Tuple2<Integer,Integer>,Tuple2<String,String>> resultSorted = result2.mapToPair(
                (Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._1()._1(), t._2()._1()), new Tuple2<>(t._1()._2(), t._2()._2()));
                }
        ).sortByKey(new yearComparator());

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedByYearGenre");

        JavaPairRDD<Object, BSONObject> save2 = resultSorted.mapToPair(
                (Tuple2<Tuple2<Integer,Integer>,Tuple2<String,String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("year", t._1()._1());
                    bson.put("rate", t._1()._2());
                    bson.put("genre", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        save2.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    ///////////////////////////////////////
    // Get top-rated movies (for men, for women, by gender activities)
    public  void topRatedFilmsGender()
    {
        // Get pairs user_id : gender
        JavaPairRDD<Integer, String> users = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[2]);
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

        // Get pairs user_id : film_id , rate
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmUserRate = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>(Integer.parseInt(line[1]), Integer.parseInt(line[2])));
                }
        );

        // Do pairs film : rate , gender
        JavaPairRDD<Integer, Tuple2<Integer, String>> genderRate = filmUserRate.join(users).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, String>> t) -> new Tuple2<>(t._2()._1()._1(), new Tuple2<>(t._2()._1()._2(), t._2()._2()))
        );

        // Do pairs film : (rate, gender), title
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmTitleRate = genderRate.join(filmTitle);

        /////////////////////////////////
        // Get top-rated films for male
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmRateMale = filmTitleRate.filter(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return t._2()._1()._2().contains("M");
                }
        );

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> sumRateMale = filmRateMale.reduceByKey(
                (Tuple2<Tuple2<Integer, String>, String> t1, Tuple2<Tuple2<Integer, String>, String> t2) -> {
                    return new Tuple2<>(new Tuple2<>(t1._1()._1() + t2._1()._1(), t1._1()._2()), t1._2());
                }
        );

        // Do pairs rate : title
        JavaPairRDD<Integer, String> resultMale = sumRateMale.mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> new Tuple2<>(t._2()._1()._1(), t._2()._2())
        ).sortByKey(false);

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongMale");

        JavaPairRDD<Object, BSONObject> saveMale = resultMale.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("rate", t._1());
                    bson.put("title", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveMale.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        /////////////////////////////////
        // Get top-rated films for female
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmRateFemale = filmTitleRate.filter(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return t._2()._1()._2().contains("F");
                }
        );

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> sumRateFemale = filmRateFemale.reduceByKey(
                (Tuple2<Tuple2<Integer, String>, String> t1, Tuple2<Tuple2<Integer, String>, String> t2) -> {
                    return new Tuple2<>(new Tuple2<>(t1._1()._1() + t2._1()._1(), t1._1()._2()), t1._2());
                }
        );

        // Do pairs rate : title
        JavaPairRDD<Integer, String> resultFemale = sumRateFemale.mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> new Tuple2<>(t._2()._1()._1(), t._2()._2())
        ).sortByKey(false);

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongFemale");

        JavaPairRDD<Object, BSONObject> saveFemale = resultFemale.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("rate", t._1());
                    bson.put("title", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveFemale.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        //////////////////////////////////
        //Get top-rated for occupation

        // Get pairs user_id : occupation
        JavaPairRDD<Integer,String> userOcc = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[3]);
                }
        );

        // Do pairs film : rate , occ
        JavaPairRDD<Integer, Tuple2<Integer, String>> occRate = filmUserRate.join(userOcc).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, String>> t) -> new Tuple2<>(t._2()._1()._1(), new Tuple2<>(t._2()._1()._2(), t._2()._2()))
        );

        // Do pairs film, occ : rate, title
        JavaPairRDD<Tuple2<Integer, String>,Tuple2<Integer, String>> filmOccRate = occRate.join(filmTitle).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._1(), t._2()._1()._2()),new Tuple2<>(t._2()._1()._1(), t._2()._2()));
                }
        );

        JavaPairRDD<Tuple2<Integer, String>,Tuple2<Integer, String>> sumRateOcc = filmOccRate.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> new Tuple2<>(t1._1()+ t2._1(), t1._2())
        );

        // Do pairs occupation : rate, title
        JavaPairRDD<String, Tuple2<Integer, String>> occSumRate = sumRateOcc.mapToPair(
                (Tuple2<Tuple2<Integer, String>,Tuple2<Integer, String>> t) -> new Tuple2<>(t._1()._2(), new Tuple2<>(t._2()._1(), t._2()._2()))
        );

        JavaPairRDD<String, Tuple2<Integer, String>> resultOcc = occSumRate.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1()> t2._1()) return t1;
                    else return t2;
                }
        );

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongOccupation");

        JavaPairRDD<Object, BSONObject> saveOcc = resultOcc.mapToPair(
                (Tuple2<String, Tuple2<Integer, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("occupation", t._1());
                    bson.put("rate", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveOcc.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    //////////////////////////////////////
    // Get most discussed films (for men, for women, by gender activities))
    public  void topDiscussedFilms()
    {
        // Get pairs user_id : gender
        JavaPairRDD<Integer, String> users = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[2]);
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

        // Get pairs user_id : film_id , reviews
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmUserReviews = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>(Integer.parseInt(line[1]), 1));
                }
        );

        // Do pairs film : reviews , gender
        JavaPairRDD<Integer, Tuple2<Integer, String>> genderReviews = filmUserReviews.join(users).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, String>> t) -> new Tuple2<>(t._2()._1()._1(), new Tuple2<>(t._2()._1()._2(), t._2()._2()))
        );

        // Do pairs film : (reviews, gender), title
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmTitleReviews = genderReviews.join(filmTitle);

        /////////////////////////////////
        // Get top-reviews films for male
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmReviewsMale = filmTitleReviews.filter(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return t._2()._1()._2().contains("M");
                }
        );

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> sumReviewsMale = filmReviewsMale.reduceByKey(
                (Tuple2<Tuple2<Integer, String>, String> t1, Tuple2<Tuple2<Integer, String>, String> t2) -> {
                    return new Tuple2<>(new Tuple2<>(t1._1()._1() + t2._1()._1(), t1._1()._2()), t1._2());
                }
        );

        // Do pairs reviews : title
        JavaPairRDD<Integer, String> resultMale = sumReviewsMale.mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> new Tuple2<>(t._2()._1()._1(), t._2()._2())
        ).sortByKey(false);

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopReviewsAmongMale");

        JavaPairRDD<Object, BSONObject> saveMale = resultMale.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("reviews", t._1());
                    bson.put("title", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveMale.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        /////////////////////////////////
        // Get top-reviews films for female
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> filmReviewsFemale = filmTitleReviews.filter(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return t._2()._1()._2().contains("F");
                }
        );

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, String>, String>> sumReviewsFemale = filmReviewsFemale.reduceByKey(
                (Tuple2<Tuple2<Integer, String>, String> t1, Tuple2<Tuple2<Integer, String>, String> t2) -> {
                    return new Tuple2<>(new Tuple2<>(t1._1()._1() + t2._1()._1(), t1._1()._2()), t1._2());
                }
        );

        // Do pairs reviews : title
        JavaPairRDD<Integer, String> resultFemale = sumReviewsFemale.mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> new Tuple2<>(t._2()._1()._1(), t._2()._2())
        ).sortByKey(false);

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopReviewsAmongFemale");

        JavaPairRDD<Object, BSONObject> saveFemale = resultFemale.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("reviews", t._1());
                    bson.put("title", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveFemale.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        //////////////////////////////////
        //Get top-reviews for occupation

        // Get pairs user_id : occupation
        JavaPairRDD<Integer,String> userOcc = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), line[3]);
                }
        );

        // Do pairs film : reviews , occ
        JavaPairRDD<Integer, Tuple2<Integer, String>> occReviews = filmUserReviews.join(userOcc).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, String>> t) -> new Tuple2<>(t._2()._1()._1(), new Tuple2<>(t._2()._1()._2(), t._2()._2()))
        );

        // Do pairs film, occ : reviews, title
        JavaPairRDD<Tuple2<Integer, String>,Tuple2<Integer, String>> filmOccReviews = occReviews.join(filmTitle).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, String>, String>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._1(), t._2()._1()._2()),new Tuple2<>(t._2()._1()._1(), t._2()._2()));
                }
        );

        JavaPairRDD<Tuple2<Integer, String>,Tuple2<Integer, String>> sumReviewsOcc = filmOccReviews.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> new Tuple2<>(t1._1()+ t2._1(), t1._2())
        );

        // Do pairs occupation : reviews, title
        JavaPairRDD<String, Tuple2<Integer, String>> occSumReviews = sumReviewsOcc.mapToPair(
                (Tuple2<Tuple2<Integer, String>,Tuple2<Integer, String>> t) -> new Tuple2<>(t._1()._2(), new Tuple2<>(t._2()._1(), t._2()._2()))
        );

        JavaPairRDD<String, Tuple2<Integer, String>> resultOcc = occSumReviews.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1()> t2._1()) return t1;
                    else return t2;
                }
        );

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopReviewsAmongOccupation");

        JavaPairRDD<Object, BSONObject> saveOcc = resultOcc.mapToPair(
                (Tuple2<String, Tuple2<Integer, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("occupation", t._1());
                    bson.put("reviews", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveOcc.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    //////////////////////////////////////
    // Get top-rated films (for each age group of viewers, by genre, among all)
    public void topRatedGroup()
    {
        ////////////////////////////
        // For all

        // Do pairs film_id : rate
        JavaPairRDD<Integer,Integer>  filmOneRate = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]), Integer.parseInt(line[2]));
                }
        );

        // Summarize the number of rating for each film
        JavaPairRDD<Integer,Integer> sumRate = filmOneRate.reduceByKey(
                (Integer a, Integer b) -> a + b
        );

        // Do pairs film_id : title
        JavaPairRDD<Integer, String> filmTitle = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    try {
                        return new Tuple2<>(Integer.parseInt(line[0]),line[1].substring(0,line[1].lastIndexOf("(")));
                    }
                    catch (Exception e){
                        return new Tuple2<>(Integer.parseInt(line[0]), "Unknown");
                    }
                }
        );

        JavaPairRDD<Integer, String> resultAll = sumRate.join(filmTitle).mapToPair(
                (Tuple2<Integer, Tuple2<Integer, String>> t) -> new Tuple2<>(t._2()._1(), t._2()._2())
        ).sortByKey(false);

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongAll");

        JavaPairRDD<Object, BSONObject> saveAll = resultAll.mapToPair(
                (Tuple2<Integer, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("rate", t._1());
                    bson.put("title", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveAll.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        ///////////////////////////////////
        // For each age group of viewers

        // Do pairs user_id : age
        JavaPairRDD<Integer,Integer> userAge = usersFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return new Tuple2<>(Integer.parseInt(line[0]), Integer.parseInt(line[1]));
                }
        );

        // Get pairs user_id : film_id , rate
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmUserRate = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>(Integer.parseInt(line[1]), Integer.parseInt(line[2])));
                }
        );

        // Do pairs film : rate, age
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> ageRate = filmUserRate.join(userAge).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>> t) -> new Tuple2<>(t._2()._1()._1(), new Tuple2<>(t._2()._1()._2(), t._2()._2()))
        );

        // Do pairs film, age : rate, title
        JavaPairRDD<Tuple2<Integer, Integer>,Tuple2<Integer, String>> filmAgeRate = ageRate.join(filmTitle).mapToPair(
                (Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, String>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._1(), t._2()._1()._2()),new Tuple2<>(t._2()._1()._1(), t._2()._2()));
                }
        );

        JavaPairRDD<Tuple2<Integer, Integer>,Tuple2<Integer, String>> sumRateAge = filmAgeRate.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> new Tuple2<>(t1._1()+ t2._1(), t1._2())
        );

        // Do pairs age : rate, title
        JavaPairRDD<Integer, Tuple2<Integer, String>> ageSumRate = sumRateAge.mapToPair(
                (Tuple2<Tuple2<Integer, Integer>,Tuple2<Integer, String>> t) -> new Tuple2<>(t._1()._2(), new Tuple2<>(t._2()._1(), t._2()._2()))
        );

        JavaPairRDD<Integer, Tuple2<Integer, String>> resultAge = ageSumRate.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1()> t2._1()) return t1;
                    else return t2;
                }
        ).sortByKey(false);

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongAgeGroup");

        JavaPairRDD<Object, BSONObject> saveAge = resultAge.mapToPair(
                (Tuple2<Integer, Tuple2<Integer, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("age", t._1());
                    bson.put("rate", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveAge.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );

        ///////////////////////////////////
        // by genre

        List<String> genres = genresFile.map(
                (String s) -> {
                    String[] line = s.split("[|]");
                    return line[0];
                }
        ).collect();

        // Get pairs film_id : (title, genres)
        JavaPairRDD<Integer, Tuple2<String, String>> filmGenre = filmsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("[|]");
                    StringBuilder genre = new StringBuilder();
                    for (int i = 0; i < 19; i++) {
                        if (Integer.parseInt(line[i + 5]) != 0)
                            genre.append(genres.get(i)).append(" ");
                    }
                    try {
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>(line[1].substring(0,line[1].lastIndexOf("(")), genre.toString()));
                    }
                    catch (Exception e){
                        return new Tuple2<>(Integer.parseInt(line[0]), new Tuple2<>("Unknown", "unknown"));
                    }
                }
        );

        // Get pairs genres : (rate, title)
        JavaPairRDD<String, Tuple2<Integer, String>> genreRate = sumRate.join(filmGenre).values().mapToPair(
                (Tuple2<Integer, Tuple2<String, String>> t) -> new Tuple2<>(t._2()._2(), new Tuple2<>(t._1(), t._2()._1()))
        );

        JavaPairRDD<String, Tuple2<Integer, String>> sumGenre = genreRate.reduceByKey(
                (Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) -> {
                    if (t1._1()> t2._1()) return t1;
                    else return t2;
                }
        );

        // Do pairs rate : genre, title and sort
        JavaPairRDD<Integer, Tuple2<String, String>> resultGenre = sumGenre.mapToPair(
                (Tuple2<String, Tuple2<Integer, String>> t) -> new Tuple2<>(t._2()._1(), new Tuple2<>(t._1(), t._2()._2())));

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedAmongGenres");

        JavaPairRDD<Object, BSONObject> saveGenre = resultGenre.mapToPair(
                (Tuple2<Integer, Tuple2<String, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("rate", t._1());
                    bson.put("genre", t._2()._1());
                    bson.put("title", t._2()._2());
                    return new Tuple2<>(null, bson);
                }
        );
        saveGenre.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    ///////////////////////////////////////
    // Get top-rated genres by each user
    public void topGenresByUser()
    {
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
                            genre.append(genres.get(i)).append(" ");
                    }
                    return new Tuple2<>(Integer.parseInt(line[0]),genre.toString());
                }
        );

        //Get pairs film_id : user, rate
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmUser = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]), new Tuple2<>(Integer.parseInt(line[0]), Integer.parseInt(line[2])));
                }
        );

        // Do pairs user_id, genre : rate
        JavaPairRDD<Tuple2<Integer, String>, Integer> genreRate = filmGenre.join(filmUser).values().mapToPair(
                (Tuple2<String, Tuple2<Integer, Integer>> t) -> {
                    return new Tuple2<>(new Tuple2<>(t._2()._1(), t._1()), t._2()._2());
                }
        );

        // Summarize the rating of each genre to the user
        JavaPairRDD<Tuple2<Integer, String>, Integer> sumGenreRate = genreRate.reduceByKey(
                (Integer a, Integer b) -> a + b
        );

        // Do pairs (user, rate) : genre
        JavaPairRDD<Tuple2<Integer, Integer>, String> result = sumGenreRate.mapToPair(
                (Tuple2<Tuple2<Integer, String>, Integer> t) ->{
                    return new Tuple2<>(new Tuple2<>(t._1()._1(), t._2()), t._1()._2());
                }
        ).sortByKey(new yearComparator());

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.TopRatedGenresByUsers");

        JavaPairRDD<Object, BSONObject> save = result.mapToPair(
                (Tuple2<Tuple2<Integer, Integer>, String> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("user", t._1()._1());
                    bson.put("rate", t._1()._2());
                    bson.put("genre", t._2());
                    return new Tuple2<>(null, bson);
                }
        );
        save.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    //////////////////////////////////////
    // Get the latest three genres by each user
    public void lastGenre()
    {
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
                            genre.append(genres.get(i)).append(" ");
                    }
                    return new Tuple2<>(Integer.parseInt(line[0]),genre.toString());
                }
        );

        //Get pairs film_id : user, time
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmUser = reviewsFile.mapToPair(
                (String s) -> {
                    String[] line = s.split("\t");
                    return new Tuple2<>(Integer.parseInt(line[1]), new Tuple2<>(Integer.parseInt(line[0]), Integer.parseInt(line[3])));
                }
        );

        // Group by users
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> group = filmGenre.join(filmUser).values().mapToPair(
                (Tuple2<String, Tuple2<Integer, Integer>> t) -> {
                    return new Tuple2<>(t._2()._1(), new Tuple2<>(t._2()._2(), t._1()));
                }
        ).groupByKey().sortByKey(false);

        // Get the latest three genres for each user
        JavaPairRDD<Integer,Tuple2 <Integer, String>> result = group.flatMapToPair(
                (Tuple2<Integer, Iterable<Tuple2<Integer, String>>> t) -> {
                    List<Tuple2<Integer, String>> l = new ArrayList<>();
                    Iterator<Tuple2<Integer, String>> iter = t._2().iterator();
                    while (iter.hasNext())
                        l.add(iter.next());

                    Collections.sort(l, (o1, o2) -> o2._1().compareTo(o1._1()));

                    List<Tuple2<Integer,Tuple2 <Integer, String>>> res = new ArrayList<>();
                    for (int i = 0; i < 3; i++)
                    {
                        res.add(new Tuple2<>(t._1(), new Tuple2<>(l.get(i)._1(), l.get(i)._2())));
                    }
                    return res;
                }
        );

        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/ResultDB.LastGenres");

        JavaPairRDD<Object, BSONObject> save = result.mapToPair(
                (Tuple2<Integer,Tuple2 <Integer, String>> t) -> {
                    BSONObject bson = new BasicBSONObject();
                    bson.put("genres", t._2._2);
                    bson.put("users", t._1());
                    return new Tuple2<>(null, bson);
                }
        );
        save.saveAsNewAPIHadoopFile(
                "file:///tmp",
                Object.class,
                Object.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }

    // Comparator for (year, rate) : (genre, title)
    private static class yearComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            if (o2._1()>o1._1())
                return 1;
            else if (o2._1()<o1._1())
                    return -1;
                 else return o2._2().compareTo(o1._2());
        }
    }

}