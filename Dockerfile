# Use an official Spark-Hadoop image as the base
FROM liliasfaxi/spark-hadoop:hv-2.7.2

# Set working directory
WORKDIR /usr/local/spark/app

# Copy the Python scripts and data files into the container
COPY analysis.py arbres.csv /usr/local/spark/app/

# Default command to run your analysis script
CMD ["spark-submit", "--master", "local[*]", "/usr/local/spark/app/analysis.py"]
