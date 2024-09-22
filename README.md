## Components

- `movie_analysis.py`: Python script containing the 1) ticker sentiment generator (basis articles) 2) movie similarity analysis logic and Airflow DAG definitions.
- `run.sh`: Shell script for setting up the Airflow environment and running the DAG.

## Setup

1. **Clone this repository:**
   ```bash
   git clone https://github.com/yourusername/movie-similarity-analysis.git
   cd movie-similarity-analysis
   ```

2. **Make the shell script executable:**
   ```bash
   chmod +x run.sh
   ```

3. **Run the setup script:**
   ```bash
   ./run.sh
   ```

   This script will:
   - Install necessary Python packages
   - Set up Airflow
   - Start the Airflow webserver and scheduler
   - Trigger the `generate_sentiment_dag` (PS: 2nd dag is triggered on succ completion of this)

4. **Access the Airflow web interface:**
   - URL: `http://localhost:8080`
   - Username: `admin`
   - Password: `admin`

5. **Monitor DAG execution:**
   - Use the Airflow web interface to monitor the execution of your DAGs.
