FROM bitnami/spark:3.4

# Copy script ETL và thư viện jars vào container
COPY Python_ETL_Pipeline.py /app/Python_ETL_Pipeline.py
COPY jars/*.jar /opt/bitnami/spark/jars/

WORKDIR /app

# Chạy file ETL khi container start
CMD ["/opt/bitnami/spark/bin/spark-submit", "Python_ETL_Pipeline.py"]
