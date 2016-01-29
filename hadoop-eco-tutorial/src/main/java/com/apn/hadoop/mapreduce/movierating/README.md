# Hadoop Movierating App

We use the Movie Lens dataset from Group Lens: http://grouplens.org/datasets/movielens/

The assignment was to get the top movies for all users who are Programmer, Lawyer or Retired.

**Job 1**

1. Get user data (user id, occupation)
2. Get movie ratings (user id, movie id|rating)
3. Filter users in scope and put together movie ratings (movie id, rating)

**Job 2**

4. Get all movie ratings from previous job
5. Get all movie titles (movie id, title)
6. Match the titles to the movies and calculate ratings (Movie title, Rating)

**Job 3**

7. Get all results from previous job and invert key (Rating, Title)
8. Reverse the Hadoop default key sort
9. Again reverse the key/value to end up with (Movie Title, Rating)

The end result is a list of movies sorted from highest rating to lowest rating based on people who are programmer, lawyer or retirred.
