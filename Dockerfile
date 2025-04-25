FROM apache/airflow:2.7.3

# Install pymongo (used in your DAGs)
RUN pip install pymongo
RUN pip install pandas

# Switch back to airflow user
USER airflow
