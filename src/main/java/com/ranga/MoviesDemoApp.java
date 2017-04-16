package com.ranga;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Cars Demo App
 * @author Ranga Reddy
 * @since 16-4-2017
 * @version 1.0
 */
public class MoviesDemoApp 
{
	private static JavaRDD<MoviesData> moviesDataRDD;
	
    @SuppressWarnings({ "resource", "serial" })
	public static void main( String[] args )
    {
    	if(args.length < 1) {
    		System.out.println("Usage Info: MoviesDemoApp <movies_dataset_file>");
    		System.exit(0);
    	}
    	
    	System.setProperty("hadoop.home.dir", "C:\\winutils");
    		
    	String filePath = args[0];
    	System.out.println("Movies dataset file path "+filePath);
        SparkConf config = new SparkConf().setAppName("MoviesDemoApp").setIfMissing("spark.master", "local[*]");
        
        JavaSparkContext sparkContext = new JavaSparkContext(config);
        
        JavaRDD<String> fileRDD = sparkContext.textFile(filePath);
        moviesDataRDD = fileRDD.map(new Function<String, MoviesData>() {
			@Override
			public MoviesData call(String arg0) throws Exception {
				String movies[] = arg0.split(",");
				Integer year = Integer.parseInt(movies[0]);
				Long length = Long.parseLong(movies[1]);
				String title = movies[2];
				String subject = movies[3];
				String actor = movies[4];
				String actress = movies[5];
				String director = movies[6];
				Integer popularity = Integer.parseInt(movies[7]);
				String award = movies[8];
				MoviesData moviesData = new MoviesData(year, length, title, subject, actor, actress, director, popularity, award);
				return moviesData;
			}
        	
        });
        
        displayHorrorMovies();
        displayMoviesReleasedEachYear();
        displayMoviesLengthGreaterThan100();
        displayAwardedMovies();
        displayActorDetails();
        displayMovieTitle();  
    }
	
    @SuppressWarnings("serial")
	private static void displayHorrorMovies() {
		// How many horror movies were there between the year 1952 and 1968.
    	
    	JavaRDD<MoviesData> filterRDD = moviesDataRDD.filter(new Function<MoviesData, Boolean>() {
			@Override
			public Boolean call(MoviesData moviesData) throws Exception {
				Integer year = moviesData.getYear();
				String subject = moviesData.getSubject();
				return (year >1951 && year < 1969) && "Horror".equals(subject);
			}
		});
    	System.out.println("Horror movies were there b/w year 1952 & 1968 is : \n"+filterRDD.count());
	}
    
    @SuppressWarnings("serial")
	private static void displayMoviesReleasedEachYear() {
		// How many movies were released for each year
		
    	JavaPairRDD<Integer, Integer> moviesPairRDD = moviesDataRDD.mapToPair(new PairFunction<MoviesData, Integer, Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(MoviesData arg0) throws Exception {
				return new Tuple2<Integer, Integer>(arg0.getYear(), 1);
			}
		});
    	
    	JavaPairRDD<Integer, Integer> yearRDD = moviesPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1	;
			}
    	});
    	
    	List<Tuple2<Integer, Integer>> releasedData = yearRDD.collect();
    	System.out.println("Movies released each years is :");
    	for(Tuple2<Integer, Integer> element : releasedData){
            System.out.println("("+element._1+", "+element._2+")");
        }
	}
   
    @SuppressWarnings("serial")
	private static void displayMovieTitle() {
		// List the title of the movies whose popularity reach above 60.
		
    	JavaRDD<MoviesData> moviesPopularityFilterRDD = moviesDataRDD.filter(new Function<MoviesData, Boolean>() {
			@Override
			public Boolean call(MoviesData moviesData) throws Exception {
				return moviesData.getPopularity() > 60;
			}
    	});
    	
    	List<MoviesData> popularityMovies = moviesPopularityFilterRDD.collect();
    	System.out.println("Title of the movies whose popularity reach above 60.");
    	popularityMovies.forEach(moviesData -> {
    		System.out.println(moviesData.getTitle());
    	});
	}
	
    @SuppressWarnings({ "serial" })
	private static void displayActorDetails() {
		
		// Find the name of the actor who have worked more than 1 movie.
		JavaPairRDD<String, Integer> moviesPairRDD = moviesDataRDD.mapToPair(new PairFunction<MoviesData, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(MoviesData moviesData) throws Exception {
				return new Tuple2<String, Integer>(moviesData.getActor(), 1);
			}
		});
		
		JavaPairRDD<String, Integer> reducedActorRDD = moviesPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1	;
			}
    	});

		JavaPairRDD<String, Integer> filteredActorRDD = reducedActorRDD.filter(new Function<Tuple2<String, Integer>, Boolean> () {
			@Override
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0._2 > 1;
			}
		});
		
		List<Tuple2<String, Integer>> collectedData = filteredActorRDD.collect();
		System.out.println("Actor(s) who have worked more than 1 movie: ");
		collectedData.forEach(data -> {
			System.out.println(data._1);
		});
	}
	
	@SuppressWarnings("serial")
	private static void displayAwardedMovies() {
		// List the awarded movie director names and title.
		
		JavaRDD<MoviesData> awardMoviesFilterRDD = moviesDataRDD.filter(new Function<MoviesData, Boolean>() {
			@Override
			public Boolean call(MoviesData moviesData) throws Exception {
				return "Yes".equalsIgnoreCase(moviesData.getAward());
			}
    	});
		
		List<MoviesData> awardedMoviesData = awardMoviesFilterRDD.collect();
		System.out.println("List of awarded movie director names and title : ");
		awardedMoviesData.forEach(data -> {
			System.out.println(data.getDirector() +", "+data.getTitle());
		});
	}
	
	@SuppressWarnings("serial")
	private static void displayMoviesLengthGreaterThan100() {
		// How many movie’s length greater than 100 min.
		
		JavaRDD<MoviesData> moviesLength100FilterRDD = moviesDataRDD.filter(new Function<MoviesData, Boolean>() {
			@Override
			public Boolean call(MoviesData moviesData) throws Exception {
				return moviesData.getLength() > 100;
			}
    	});
		
		System.out.println("Movies length greater than 100 min is : \n"+moviesLength100FilterRDD.count());
	}
}
