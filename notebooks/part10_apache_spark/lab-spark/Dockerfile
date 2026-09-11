FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*
 
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install \
        "pyspark==4.2.0" \
        "jupyterlab>=4,<5"
 
WORKDIR /lab
 
EXPOSE 8888 4040
 
CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=", "--ServerApp.password=", \
     "--ServerApp.root_dir=/lab"]