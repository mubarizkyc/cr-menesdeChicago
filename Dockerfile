FROM apache/spark:4.0.2

USER root

# Install Python dependencies
RUN pip install --no-cache-dir numpy

# Create data/output dirs with open permissions
RUN mkdir -p /output /data && chmod 777 /output /data

WORKDIR /app
COPY batch_layer.py /app/batch_layer.py
RUN chmod +x /app/batch_layer.py