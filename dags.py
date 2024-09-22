import urllib.request
import zipfile
import os
import pandas as pd
from scipy.stats import pearsonr
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import statistics
import time
import random
import time
import pandas as pd
import random
import sqlite3
import datetime as dt
import statistics
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


def analyse_movies():

    url = 'https://files.grouplens.org/datasets/movielens/ml-100k.zip'
    zip_path = 'ml-100k.zip'

    if not os.path.exists(zip_path):
        print("Downloading the dataset...")
        urllib.request.urlretrieve(url, zip_path)
        print("Download complete.")

    if not os.path.exists('ml-100k'):
        print("Extracting the dataset...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall()
        print("Extraction complete.")

    user_cols = ['user_id', 'age', 'gender', 'occupation', 'zip_code']
    users = pd.read_csv('ml-100k/u.user', sep='|', names=user_cols)

    rating_cols = ['user_id', 'movie_id', 'rating', 'timestamp']
    ratings = pd.read_csv('ml-100k/u.data', sep='\t', names=rating_cols)

    movie_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'IMDb_URL'] + \
                 ['unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy',
                  'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror',
                  'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    movies = pd.read_csv('ml-100k/u.item', sep='|', encoding='latin-1', names=movie_cols)

    mean_age_per_occupation = users.groupby('occupation')['age'].mean().reset_index()
    print("Mean age of users in each occupation:")
    print(mean_age_per_occupation)

    ratings_movies = pd.merge(ratings, movies, on='movie_id')
    movie_rating_counts = ratings.groupby('movie_id').size().reset_index(name='rating_count')
    popular_movies = movie_rating_counts[movie_rating_counts['rating_count'] >= 35]
    avg_ratings = ratings.groupby('movie_id')['rating'].mean().reset_index()
    avg_ratings = pd.merge(avg_ratings, movies[['movie_id', 'title']], on='movie_id')
    avg_ratings = pd.merge(avg_ratings, popular_movies, on='movie_id')
    avg_ratings = avg_ratings.sort_values('rating', ascending=False)
    top_20_movies = avg_ratings.head(20)
    print("\nTop 20 highest-rated movies (rated at least 35 times):")
    print(top_20_movies[['title', 'rating', 'rating_count']])

    def age_group(age):
        if 20 <= age < 25: return '20-25'
        elif 25 <= age < 35: return '25-35'
        elif 35 <= age < 45: return '35-45'
        elif age >= 45: return '45+'
        else: return 'Under 20'

    users['age_group'] = users['age'].apply(age_group)
    user_ratings = pd.merge(users, ratings, on='user_id')
    user_ratings_movies = pd.merge(user_ratings, movies, on='movie_id')
    
    genres = ['unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy',
              'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror',
              'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    
    melted = user_ratings_movies.melt(id_vars=['user_id', 'age', 'gender', 'occupation', 'age_group', 'movie_id', 'rating'],
                                      value_vars=genres, var_name='genre', value_name='genre_flag')
    melted_genres = melted[melted['genre_flag'] == 1]
    genre_counts = melted_genres.groupby(['occupation', 'age_group', 'genre']).size().reset_index(name='rating_count')
    top_genres = genre_counts.sort_values(['occupation', 'age_group', 'rating_count'], ascending=[True, True, False])
    top_genres = top_genres.groupby(['occupation', 'age_group']).first().reset_index()
    
    print("\nTop genres rated by users of each occupation in every age group:")
    print(top_genres[['occupation', 'age_group', 'genre']])

    user_movie_ratings = ratings.pivot_table(index='user_id', columns='movie_id', values='rating')

    target_movie_title='Mrs. Winterbourne (1996)'
    target_movie = movies[movies['title'] == target_movie_title]
    
    if target_movie.empty:
        print(f"Movie '{target_movie_title}' not found in the dataset.")
        return None

    target_movie_id = target_movie['movie_id'].values[0]
    print("\nTarget movie ID:", target_movie_id)
    
    if target_movie_id not in user_movie_ratings.columns:
        print(f"Movie ID {target_movie_id} not found in user_movie_ratings columns.")
        return None

    target_ratings = user_movie_ratings[target_movie_id]
    similarity_scores = []
    co_occurrence_threshold = 10
    similarity_threshold = 0.5

    for movie_id in user_movie_ratings.columns:
        if movie_id == target_movie_id:
            continue
        movie_ratings = user_movie_ratings[movie_id]
        common_users = target_ratings.notnull() & movie_ratings.notnull()
        co_occurrence = common_users.sum()
        if co_occurrence >= co_occurrence_threshold:
            sim, _ = pearsonr(target_ratings[common_users], movie_ratings[common_users])
            if not pd.isna(sim) and sim >= similarity_threshold:
                similarity_scores.append({'movie_id': movie_id, 'score': sim, 'strength': co_occurrence})

    similar_movies = pd.DataFrame(similarity_scores)
    if not similar_movies.empty:
        similar_movies = pd.merge(similar_movies, movies[['movie_id', 'title']], on='movie_id')
        similar_movies = similar_movies.sort_values('score', ascending=False)
        top_10_similar = similar_movies.head(10)
        print(f"\nTop 10 similar movies for '{target_movie_title}':")
        for index, row in top_10_similar.iterrows():
            print(f"{row['title']:<50} score: {row['score']:<20} strength: {row['strength']}")
    else:
        print("No similar movies found with the given thresholds.")

analyse_movies()

# from airflow import DAG
# from airflow.decorators import task


def generate_sentiment(ticker):
    
    def setup_driver():
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  
        chrome_options.add_argument("--window-size=1920,1080")
        return webdriver.Chrome(options=chrome_options)
    
    def scrape_yourstory(search_term):

        def safe_find_element(driver, by, value, timeout=10):
            try:
                return WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )
            except TimeoutException:
                print(f"Element not found: {by}={value}")
                return None

        driver = setup_driver()
        articles = []
        scraped_urls = set()
        
        try:
            driver.get("https://yourstory.com/search")
            
            search_input = safe_find_element(driver, By.ID, "q")
            if search_input:
                search_input.send_keys(search_term)
                search_input.send_keys(Keys.RETURN)
                print(f"Searched YourStory for: {search_term}")
            else:
                print("Failed to find search input on YourStory")
                return articles
    
            time.sleep(5)
            
            container_results = safe_find_element(driver, By.CLASS_NAME, 'container-results')
            if not container_results:
                print("Failed to find container-results on YourStory")
                return articles
            
            article_container = container_results.find_elements(By.TAG_NAME, 'div')[1]
            links = article_container.find_elements(By.TAG_NAME, 'a')
            
            for link in links:
                if len(articles) >= 5:
                    break
                
                url = link.get_attribute('href')
                if url in scraped_urls:
                    continue
                
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(url)
                
                title_element = safe_find_element(driver, By.CSS_SELECTOR, 'h1.article-title')
                title = title_element.text.strip() if title_element else "No title found"
                
                content = safe_find_element(driver, By.CSS_SELECTOR, 'div[class*="quill-content"]')
                content_text = content.text if content else "Content not found"
                
                articles.append({
                    'title': title,
                    'content': content_text,
                    'url': url,
                    'source': 'YourStory',
                    'search_term': search_term
                })
                scraped_urls.add(url)
                
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(random.uniform(1, 3))
            
        finally:
            driver.quit()
        
        return articles
        
    def scrape_finshots(search_term):
        def setup_driver():
            chrome_options = Options()
            chrome_options.add_argument("--window-size=1920,1080")
            return webdriver.Chrome(options=chrome_options)
        
        def safe_find_element(driver, by, value, timeout=10):
            try:
                return WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )
            except TimeoutException:
                print(f"Element not found: {by}={value}")
                return None
    
        driver = setup_driver()
        articles = []
        
        try:
            driver.get("https://finshots.in")
            
            search_button = safe_find_element(driver, By.CSS_SELECTOR, 'a.toggle-search-button.js-search-toggle')
            if search_button:
                search_button.click()
            else:
                print("Failed to find search button on Finshots")
                return articles
    
            search_input = safe_find_element(driver, By.CSS_SELECTOR, 'input[type="search"]')
            if search_input:
                search_input.send_keys(search_term)
                print(f"Searched Finshots for: {search_term}")
            else:
                print("Failed to find search input on Finshots")
                return articles
    
            time.sleep(5)
    
            result_links = driver.find_elements(By.CSS_SELECTOR, 'a.c-search-result')
            
            for link in result_links[:5]:  # Limit to first 5 results
                url = link.get_attribute('href')
                
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(url)
                
                title_element = safe_find_element(driver, By.CSS_SELECTOR, 'h1.post-full-title')
                title = title_element.text.strip() if title_element else "No title found"
                
                content_element = safe_find_element(driver, By.CSS_SELECTOR, 'section.post-full-content')
                content = content_element.text.strip() if content_element else "Content not found"
                
                articles.append({
                    'title': title,
                    'content': content,
                    'url': url,
                    'source': 'Finshots',
                    'search_term': search_term
                })
                
                print(f"Scraped article: {title}")
                
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"Error scraping Finshots: {str(e)}")
        
        finally:
            driver.quit()
        
        return articles
    
    def combine_articles(yourstory_articles, finshots_articles):
        all_articles = yourstory_articles + finshots_articles
        for article in all_articles:
            print(f"Combined Article Title: {article['title']}")
        return all_articles
    
    def persist_data(articles, overall_sentiment, ticker):
            conn = sqlite3.connect('hdfc_news.db')
            c = conn.cursor()
    
            c.execute('''CREATE TABLE IF NOT EXISTS articles
                         (date TEXT, ticker TEXT, title TEXT, content TEXT, url TEXT, source TEXT, sentiment_score REAL)''')
    
            c.execute('''CREATE TABLE IF NOT EXISTS overall_sentiment
                         (date TEXT, ticker TEXT, overall_sentiment REAL)''')
    
            for article in articles:
                c.execute("INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                           ticker,
                           article['title'],
                           article['content'],
                           article['url'],
                           article['source'],
                           article['sentiment_score']))
    
            c.execute("INSERT INTO overall_sentiment VALUES (?, ?, ?)",
                      (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       ticker,
                       overall_sentiment))
    
            conn.commit()
            conn.close()
    
    def mock_sentiment_api(text):
        return random.uniform(0, 1)

    def analyze_sentiment(articles):
        for article in articles:
            article['sentiment_score'] = mock_sentiment_api(article['content'])

        overall_sentiment = statistics.mean([a['sentiment_score'] for a in articles])

        return articles, overall_sentiment
        
    def scrape_ticker(ticker):
        print("here")
        yourstory_articles = scrape_yourstory(ticker)
        finshots_articles = scrape_finshots(ticker)
        print("done")
        
        return finshots_articles + yourstory_articles
    
    def scrape_and_analyze_ticker(ticker):
        articles = scrape_ticker(ticker)
        articles_with_sentiment, overall_sentiment = analyze_sentiment(articles)
        persist_data(articles_with_sentiment, overall_sentiment, ticker)
        print(f"Scraped and analyzed {len(articles_with_sentiment)} articles about {ticker}")
        print(f"Overall sentiment score for {ticker}: {overall_sentiment}")

    scrape_and_analyze_ticker(ticker)
    
generate_sentiment('hdfc')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['shivam01anand@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'generate_sentiment_dag',
    default_args=default_args,
    schedule_interval='@daily',
    description='DAG for generating sentiment',
    catchup=False,
) as dag1:

    generate_sentiment_task = PythonOperator(
        task_id='generate_sentiment_task',
        python_callable=generate_sentiment,
        op_kwargs={'ticker': '{{ dag_run.conf["ticker"] if dag_run else "hdfc" }}'},
    )

    trigger_analyse_movies_dag = TriggerDagRunOperator(
        task_id='trigger_analyse_movies_dag',
        trigger_dag_id='analyse_movies_dag',
        reset_dag_run=True,
        wait_for_completion=False,
    )

    generate_sentiment_task >> trigger_analyse_movies_dag

with DAG(
    'analyse_movies_dag',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered by the first DAG
    description='DAG for analyzing movies',
    catchup=False,
) as dag2:

    analyse_movies_task = PythonOperator(
        task_id='analyse_movies_task',
        python_callable=analyse_movies,
    )

