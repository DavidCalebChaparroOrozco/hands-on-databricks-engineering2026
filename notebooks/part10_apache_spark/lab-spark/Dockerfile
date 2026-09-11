FROM python:3.12-slim-bookworm

# JDK 17: Spark runs on the JVM. Spark scripts require procps.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# pyspark includes Apache Spark. jupyterlab is for running the notebook.
RUN pip install --no-cache-dir pyspark jupyterlab

WORKDIR /lab

# 8888 JupyterLab, 4040 Spark UI
EXPOSE 8888 4040

# Without token or password: local use.
CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=", "--ServerApp.password=", \
     "--ServerApp.root_dir=/lab"]
