package com.basic.kafka.Basic_Kafka.core_java;


import lombok.Getter;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Getter
class Movie implements Comparable<Movie>{

    private String name;
    private double rating;
    private LocalDate releaseDate;

    public Movie(String name, double rating, LocalDate releaseDate) {
        this.name = name;
        this.rating = rating;
        this.releaseDate = releaseDate;
    }



    @Override
    public String toString() {
        return "Movie{" +
                "name='" + name + '\'' +
                ", rating=" + rating +
                ", releaseDate=" + releaseDate +
                '}';
    }

    /**
     * @param o the object to be compared.
     * @return
     */
    @Override
    public int compareTo(Movie o) {
        return Double.compare( o.rating, this.rating);
    }
}

public class Interview {


    public static void main(String[] args) {
        List<String> myList = Arrays.asList("BOB", "Bieber", "zeba");
        List<String> result =  myList.stream().filter(ll -> !ll.contains("zeba")).collect(Collectors.toList());
        result.forEach(System.out::println);

        //collection of movie
        //name, rating, year of release



        Movie movie1 = new Movie("Anime", 9.2, LocalDate.of(2023, 1,29));
        Movie movie2 = new Movie("Horror", 3.7, LocalDate.of(2023, 6,23));
        Movie movie3 = new Movie("Thriller", 7,LocalDate.of(2024, 2,26));
        Movie movie4 = new Movie("Solo", 2.6, LocalDate.of(2023, 8,12) );

        Movie movie5 = new Movie("Drama", 2.6, LocalDate.of(2024, 6,13) );

        List<Movie> movies = Arrays.asList(movie1, movie2, movie3, movie4, movie5);

        List<Movie> movieResult = fetchMovieByRating(2023, movies);
        Collections.sort(movieResult);
        movieResult.forEach(System.out::println);

        Map<String, List<String>> people = new HashMap<>();
        people.put("John", Arrays.asList("555-1123", "555-3389"));
        people.put("Mary", Arrays.asList("555-2243", "555-5264"));
        people.put("Steve", Arrays.asList("555-6654", "555-3242"));

        new ArrayList<>(people.values());

    }


    public static List<Movie> fetchMovieByRating(int releaseYear, List<Movie>  movies) {

        return movies.stream().filter(movie -> movie.getReleaseDate().getYear() == releaseYear)
//                .sorted(Comparator.comparingDouble(Movie::getRating)
//                        .reversed())
                .collect(Collectors.toList());
    }


}
