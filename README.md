# Tree Data Analysis with PySpark in Docker

This project analyzes tree data using PySpark, running inside a Docker container. The data is sourced from `arbres.csv`.

## Project Structure

- `analysis.py`: Main PySpark script for analyzing tree data.
- `arbres.csv`: Data file containing information about trees.
- `Dockerfile`: Optional Dockerfile for setting up the environment.

## How to Run the Project

1. Clone this repository:
   ```bash
   git clone https://github.com/abderrahmane-stack/spark-docker-analysis.git
   cd spark-docker-analysis
   
2. Build the Docker image:

   ```bash
   docker build -t spark-analysis .

3. Run the Docker container:

  ```bash
  docker run -it spark-analysis
