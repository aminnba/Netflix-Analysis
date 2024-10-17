import pandas as pd
import sqlite3
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

df = pd.read_csv('netflix_titles.csv')


def create_database():
  conn = sqlite3.connect('movies.db')  # This will create a new SQLite database
  c = conn.cursor()

  c.execute('''CREATE TABLE IF NOT EXISTS movies (
   show_id TEXT PRIMARY KEY,
   type TEXT,
   title TEXT NOT NULL,
   director TEXT,
   cast TEXT DEFAULT NULL,  
   country TEXT,
   date_added TEXT,  
   release_year INTEGER,
   rating TEXT,
   duration TEXT,  
   listed_in TEXT,
   description TEXT
   )''')

  conn.commit()


def import_csv_to_sqlite(csv_file, database_path):
  # Connect to the SQLite database
  conn = sqlite3.connect(database_path)

  # Read the CSV file into a DataFrame
  df = pd.read_csv(csv_file)

  # Insert DataFrame into SQLite table
  try:
    df.to_sql('movies', conn, if_exists='append', index=False)
    print("Data imported successfully.")
  except Exception as e:
    print(f"Error importing data: {e}")
  # Close the connection
  conn.close()


def query_sqlite_data(database_path, query):
  # Connect to the SQLite database
  conn = sqlite3.connect(database_path)

  # Execute the query and store the result in a DataFrame
  df = pd.read_sql_query(query, conn)

  # Close the connection
  conn.close()

  return df


import_csv_to_sqlite('netflix_titles.csv', 'movies.db')
# Query the SQLite database
query = "SELECT * FROM movies"
movies_df = query_sqlite_data('movies.db', query)
print(movies_df.head())
country_counts = movies_df['country'].value_counts()
country_counts.head(10).plot(kind='bar', figsize=(10, 6))
plt.title('Top 10 Countries by Number of Movies')
plt.xlabel('Country')
plt.ylabel('Number of Movies')
plt.xticks(rotation=45)
plt.savefig('country_movie_distribution.png')
print("Plot saved as 'country_movie_distribution.png'")

# Conclusion
# Now let's add the analysis for the top 10 countries by number of movies.
print("""
### Country Movie Distribution Analysis:

- **United States** has the highest number of movies in the dataset, which is expected considering the domination of the entertaiment field, It also indicats its strong influence in global movie production and distribution.
- **India** comes in second place, which is intresting as we can see the Bollywood scean strong production , **United Kingdom**, and **Canada** are also major players in the entertainment industry, contributing to a wide range of genres.
- **France** and **Germany** appear in the top ranks, showing their significant role in European cinema.
- Countries like **Romania**, **Portugal**, and **New Zealand** appear lower in the list, which could suggest smaller or emerging film industries.
- In conclusion,We can see that the United States is the most dominant country in the entertainment industry by a big margin, then other countries like India, France, Germany, and Romania are also prominent.
""")

#missing data
missing_values = movies_df.isnull().sum()
print("Missing values in each column:\n", missing_values)

#clean data
movies_df.fillna({'director': 'Unknown', 'cast': 'Unknown', 'country': 'Unknown'}, inplace=True)

df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# Extract month and year from 'date_added'
df['year_added'] = df['date_added'].dt.year
df['month_added'] = df['date_added'].dt.month

print(df[['title', 'date_added', 'year_added', 'month_added']].head())

# Code for checking and handling missing values

missing_values = movies_df.isnull().sum()
print("Missing values in each column:\n", missing_values)

# Data cleaning steps
# Fill missing values with 'Unknown'
movies_df['director'] = movies_df['director'].fillna('Unknown')
movies_df['cast'] = movies_df['cast'].fillna('Unknown')
movies_df['country'] = movies_df['country'].fillna('Unknown')

# Convert 'date_added' to datetime format, handling errors by setting invalid parsing as NaT
movies_df['date_added'] = pd.to_datetime(movies_df['date_added'], errors='coerce')


# Extract month and year from 'date_added'
movies_df['year_added'] = movies_df['date_added'].dt.year
movies_df['month_added'] = movies_df['date_added'].dt.month

print(movies_df[['title', 'date_added', 'year_added', 'month_added']].head())

# Conclusion markdown for missing data handling and cleaning:
print("""
### Missing Data & Data Cleaning

- **Missing Data**: 
    - I identified that the dataset has some missing values in columns such as `director`, `cast`, and `country`. These are important columns for understanding the movies in terms of their creators and origin.
    - Filling missing values with "Unknown" allows us to keep the dataset intact without dropping potentially useful data, ensuring we can still perform meaningful analysis.

- **Data Cleaning**:
    - We converted the `date_added` column to datetime format, which allows us to extract more useful insights such as the year and month a movie was added.
    - This is important because trends over time are easier to observe when the data is in the correct format.

- **Conclusion**:
    - After cleaning the dataset, we now have a reliable dataset to work with, free from the issues that would arise from missing data. This process sets the stage for deeper analyses of trends and patterns over time .

- **Next Step**: With the cleaned data, we can start to analyze trends in movies over time
""")

# how many movies added each year
year_counts = df['year_added'].value_counts().sort_index()
year_counts.plot(kind='bar', figsize=(10, 6))
plt.title('Number of Movies Added Each Year')
plt.xlabel('Year')
plt.ylabel('Number of Movies')
plt.xticks(rotation=45)
plt.savefig('year_movie_distribution.png')
print("Plot saved as 'year_movie_distribution.png'")

### Analysis: Number of Movies Added Each Year
print("""
**Goal:** To identify trends in the number of movies added to the database over the years.

1. **Tools Used:**
   - **Pandas**: For loading and manipulating the data.
   - **Matplotlib**: For generating the bar chart.

2. **Approach:**
   - We extracted the year from the `date_added` column to determine when each movie was added to the database.
   - A bar chart was created to visualize the number of movies added per year.

3. **Results:**
   - The chart shows the distribution of movies added over the years, with clear peaks and troughs, indicating varying levels of content being added across different periods.
4. **Conclusion:**
   - We can see from the chart that there 2008 had the most movies added, that can be attributed to the introduction of the streaming service, allowing subscribers to instantly watch television shows and movies on their personal computers.
As part of this transition, Netflix added a large number of titles to its platform around 2008 and started aggressively acquiring licenses for movies and TV shows to grow its streaming library. This period marked Netflix’s shift from a DVD rental company to a streaming giant, necessitating a larger library to attract users to the streaming service. 
""")

#converting duration of the movie to numeric



# Top 10 directors with the most movies

filtered_movies_df = movies_df[movies_df['director'] != 'Unknown']

filtered_movies_df['director'] = filtered_movies_df['director'].str.split(',')

# Explode the 'director' column into separate rows
exploded_directors_df = filtered_movies_df.explode('director')

# Strip whitespace around names, if necessary
exploded_directors_df['director'] = exploded_directors_df[
    'director'].str.strip()

# Reset the index after exploding
exploded_directors_df.reset_index(drop=True, inplace=True)

# Get the top 10 directors based on the count
top_directors = exploded_directors_df['director'].value_counts().head(10)

plt.figure(figsize=(10, 6))
top_directors.plot(kind='bar', color='purple')
plt.title('Top 10 Directors with the Most Movies')
plt.xlabel('Director')
plt.ylabel('Number of Movies')
plt.xticks(rotation=15, ha='right')
plt.savefig('top_directors_movies.png')
print("Plot saved as 'top_directors_movies.png'")

print("""
**Goal:** To identify the top 10 directors who have directed the most movies in the dataset.

1. **Tools Used:**
   - **Pandas**: For filtering and analyzing the data.
   - **Matplotlib**: For generating the bar chart.

2. **Approach:**
   - First, we filtered out the entries where the director was marked as 'Unknown'.
   - We used `value_counts()` to count the number of movies directed by each director.
   - A bar chart was plotted to visualize the top 10 directors with the most movies.

3. **Results:**
   - The chart shows the top 10 directors, with each director's total number of movies clearly displayed.
   - Some directors consistently have a high number of movies in the dataset, which could indicate prolific careers or a particular focus on specific genres or platforms.

4. **Conclusion:**
   - We observe that Rajiv chilaka is the top director with the most movies, He is the CEO and founder of GreenGold animation which direct and produce various indian movies, that can corelate with the number of movies comming from india checked ealier, next we have Jan Suter, Raul Campos and on and on. 
   - The data provides insights into the most active directors, which could be useful for further analysis on their styles, genres, or the distribution of their works across different platforms.

""")

#Analyze movie by release year.

release_year_counts = movies_df['release_year'].value_counts().sort_index()
plt.figure(figsize=(15, 6))
release_year_counts.plot(kind='bar', color='purple')
plt.title('Movie Release Years')
plt.xlabel('Year')
plt.ylabel('Number of Movies')
plt.xticks(rotation=90)
plt.savefig('release_year_movies.png')
print("Plot saved as 'release_year_movies.png'")

print("""
**Goal:** To analyze the distribution of movies released across different years.

1. **Tools Used:**
   - **Pandas**: For loading and manipulating the data.
   - **Matplotlib**: For creating the bar chart visualization.

2. **Approach:**
   - We extracted the `release_year` column from the dataset and used `value_counts()` to determine how many movies were released in each year.
   - A bar chart was generated to visualize the distribution of movie releases over the years.

3. **Results:**
   - The chart displays the number of movies released by year. The distribution shows periods of high and low release activity, highlighting certain peaks in movie production or acquisition.

4. **Conclusion:**
   -We can see that the movie release is expenancing but relatively stable until the 90's, reflecting changes in production, distribution, or the introduction of new technologies that led to an increase in movie releases. and reaches it's peak at 2018 then starts going down, it can be to various reasons, possibly due to disruptions in the industry (e.g., economic factors or the COVID-19 pandemic).

""")

#Analyze the distribution of movie ratings.

rating_counts = movies_df['rating'].value_counts()
plt.figure(figsize=(10, 6))
rating_counts.plot(kind='bar', color='purple')
plt.title('Movie Ratings')
plt.xlabel('Rating')
plt.ylabel('Number of Movies')
plt.xticks(rotation=45)
plt.savefig('rating_distribution.png')
print("Plot saved as 'rating_distribution.png'")

print("""

**Goal:** To understand the distribution of movie ratings.

1. **Tools Used:**
   - **Pandas**: For counting the occurrences of each rating.
   - **Matplotlib**: For generating a bar chart to visualize the distribution.

2. **Approach:**
   - We used the `value_counts()` function to count the number of movies with each rating.
   - A bar chart was created to display the distribution of these ratings.

3. **Results:**
   - The chart displays the frequency of different movie ratings.

4. **Conclusion:**
   - The dominance of TV-MA and TV-14 suggests that a significant portion of the movies in the dataset are aimed at mature audience. TV-PG and PG, while present, seem to target younger viewers, and R-rated movies make up a smaller proportion compared to the others and on and on.
""")



#Analyze the Most Popular Genres.

movies_df['genres'] = movies_df['listed_in'].str.split(',')

exploded_genres = movies_df.explode('genres')

# Step 3: Count the frequency of each genre
genres = exploded_genres['genres'].value_counts().head(10)

plt.figure(figsize=(10, 6))
bar_positions = range(len(genres))
plt.bar(bar_positions, genres, width=0.5, color='purple')
plt.xticks(bar_positions, genres.index, rotation=20, ha='right')
plt.xlabel('Genre')
plt.ylabel('Number of Movies')
plt.xticks(rotation=20)
plt.savefig('most_popular_genres.png')
print("Plot saved as 'most_popular_genres.png'")

print("""
**Goal:** To identify the most popular genres in the dataset.

1. **Tools Used:**
   - **Pandas**: For splitting and exploding the genre data for analysis.
   - **Matplotlib**: For generating a bar chart to visualize the top genres.

2. **Approach:**
   - We split the `listed_in` column, which contains genres, into individual genres for each movie.
   - The data was then exploded, meaning each genre for a movie was treated as a separate entry.
   - A frequency count of the top 10 most popular genres was performed.

3. **Results:**
   - The bar chart displays the 10 most common genres, indicating their popularity within the dataset.

4. **Conclusion:**
   - The most popular genres indicate a diverse offering, with genres like Dramas as the most popular, Comedies, then action and adventures dominating the dataset. This reflects a general audience preference for these categories and the kinds of movies that are most frequently produced or acquired by the platform.
""")


#Analyze the duration of each movie

movies_only_df = movies_df[~movies_df['duration'].str.
                           contains('Seasons', na=False)].copy()
# Ensure the 'duration' column is numeric
movies_only_df['duration_numeric'] = pd.to_numeric(
    movies_df['duration'].str.replace(' min', '', regex=False),
    errors='coerce')

plt.figure(figsize=(10, 6))
plt.hist(movies_only_df['duration_numeric'].dropna(),
         bins=30,
         color='purple',
         edgecolor='black')
plt.title('Distribution of Movie Durations')
plt.xlabel('Duration (minutes)')
plt.ylabel('Frequency')
plt.grid(axis='y', alpha=0.75)
plt.savefig('duration_histogram.png')
print("Plot saved as 'duration_histogram.png'")

#Analyze the distribution of movie durations.
print("""
**Goal:** To analyze the distribution of movie durations in the dataset.

1. **Tools Used:**
   - **Pandas**: For data manipulation, specifically converting the movie duration to numeric values.
   - **Matplotlib**: For generating the histogram of movie durations.

2. **Approach:**
   - The `duration` column was converted from a string format (e.g., "90 min") to a numeric value (minutes) using string extraction.
   - A histogram was plotted to visualize the frequency of movies based on their duration.

3. **Results:**
   - The histogram reveals that most movies tend to have a duration between 80 and 120 minutes, with the frequency declining for movies longer than 100 minutes.

4. **Conclusion:**
   - The data indicates that the majority of movies in the dataset are within the 80-120 minute range, which aligns with industry norms for feature films. 
   - This visualization helps us understand common trends in movie durations and their distribution in the dataset.

""")

#Analyze the movie ratings based on the duration.
# Step 1: Extract numeric values from the 'duration' column (if not done already)
movies_df['duration_numeric'] = movies_df['duration'].str.extract('(\d+)').astype(float)

# Filter out rows where 'duration_numeric' is NaN
valid_ratings_df = movies_df[movies_df['duration_numeric'].notna()]

# Group by rating and calculate average movie duration
avg_duration_by_rating = valid_ratings_df.groupby('rating')['duration_numeric'].mean()

# Plot the results
plt.figure(figsize=(12, 6))
avg_duration_by_rating.plot(kind='bar', color='purple')
plt.title('Average Movie Duration by Rating')
plt.xlabel('Rating')
plt.ylabel('Average Duration (minutes)')
plt.xticks(rotation=45)
plt.grid(axis='y', alpha=0.75)
plt.savefig('avg_duration_by_rating.png')
print("Plot saved as 'avg_duration_by_rating.png'")

print("""
**Goal**: To analyze the average duration of movies based on their ratings to see if there is a trend between content ratings and movie lengths.

1. **Tools Used**:
   - **Pandas**: For data manipulation.
   - **Matplotlib**: For generating the bar chart.

2. **Approach**:
   - We cleaned the `rating` column by ensuring that only valid ratings were included, removing any numeric entries that might have been misinterpreted as ratings (e.g., 66 min).
   - We then calculated the average movie duration for each valid rating.
   - A bar chart was created to visualize the average duration of movies based on their rating.

3. **Results**:
   - The ratings NC-17 and UR (Unrated) have the longest average movie durations, with NC-17 exceeding 120 minutes.
   - PG-13 and R-rated movies have similar average durations, with UR movies slightly longer than PG-13.

4. **Conclusion**:
   - Movies with the NC-17 rating tend to be longer on average, potentially due to more mature or complex content requiring additional runtime.
   - The similarity in duration between PG-13, R, and UR movies suggests that mainstream movies for older audiences generally fall within a certain duration range.
   - This analysis highlights possible trends between content maturity and movie runtime, but more investigation would be needed to understand the factors influencing this pattern.
""")

# SQL Query for Popular Ratings by Year without the percentage column
conn = sqlite3.connect('movies.db')
query = '''
SELECT 
    release_year, 
    rating, 
    COUNT(*) AS movie_count
FROM movies m
GROUP BY release_year, rating
ORDER BY release_year, movie_count DESC;
'''

# Execute the query
ratings_by_year_df = pd.read_sql_query(query, conn)

# Close the connection after query execution
conn.close()

# Print confirmation and show the first few rows of the DataFrame
print("Query finished. Here are the first 5 rows:")
print(ratings_by_year_df.head())

print("""
**Goal**: To analyze the number of movies for each rating by year, allowing us to see how the distribution of ratings has changed over time.

1. **Tools Used**:
   - **SQLite**: To query the database for movie counts by release year and rating.
   - **Pandas**: For loading and manipulating the queried data.

2. **Approach**:
   - We performed an SQL query that grouped the movies by their release year and rating, then counted how many movies were released for each rating in each year.
   - The result was ordered by release year and then by the movie count, to see which ratings were most popular in each year.

3. **Results**:
   - The table shows a breakdown of how many movies were released for each rating by year. This gives insight into trends for different ratings over time.
   - You can observe the dominance of specific ratings like TV-MA, PG-13, and R in certain periods, reflecting the changing landscape of movie content.

   
""")

# Count of Movies by Genre and Rating
