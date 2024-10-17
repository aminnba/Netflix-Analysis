import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

conn = sqlite3.connect(
    'movies.db')  # Assuming you have the same database setup
movies_df = pd.read_sql_query("SELECT * FROM movies", conn)
conn.close()

