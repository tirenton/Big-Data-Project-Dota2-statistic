FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

USER jovyan

# Install pipeline dependencies so we can run the collector/processor directly from Jupyter
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create notebooks directory
RUN mkdir -p /home/jovyan/work/notebooks

WORKDIR /home/jovyan/work
