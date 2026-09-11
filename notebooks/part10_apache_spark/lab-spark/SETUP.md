# Spark Lab — Setup

Requirement: Docker Desktop installed and running.

## 1. Files

`lab-spark` folder with the following files:

```text
lab-spark/
├── Dockerfile
├── docker-compose.yml
├── spark_lab_practice.ipynb
└── SETUP.md
```

The dataset is downloaded from the notebook and saved in this folder.

## 2. Start

From a terminal, inside the folder:

```bash
cd lab-spark
docker compose up --build
```

The first time, the image will be built (~1 GB). Subsequent startups will take only a few seconds.

It is ready when the console shows:

```text
spark-lab  | http://0.0.0.0:8888/lab
```

Keep this terminal open.

## 3. Open

| URL                   | What it is | Available                               |
| --------------------- | ---------- | --------------------------------------- |
| http://localhost:8888 | JupyterLab | As soon as the container starts         |
| http://localhost:4040 | Spark UI   | After running Mission 3 in the notebook |

In JupyterLab, open `spark_lab_practice.ipynb`.

Note that `localhost:4040` will return `ERR_EMPTY_RESPONSE` until a Spark session exists. The UI is created by the Spark session, not by the container.

## 4. Working

* Run the cells in order using `Shift + Enter`.
* Refresh the Spark UI between missions.
* Missions 1 and 2 only need to be run once; if the files already exist, the notebook will detect them.

## 5. Stop and Restart

To stop: press `Ctrl + C` in the container terminal, or run `docker compose down` from the folder.

The files and dataset will remain in the local folder.

To restart: run `docker compose up` (without `--build`).

## Common Issues

| Symptom                                          | Solution                                                                                          |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `docker daemon is not running`                   | Open Docker Desktop and wait for it to finish starting                                            |
| `localhost:4040` → `ERR_EMPTY_RESPONSE`          | Run Mission 3 and refresh                                                                         |
| Mission 3 was executed but 4040 does not respond | There is another active session in another kernel; use `localhost:4041` or close the other kernel |
| `port is already allocated`                      | Close the program using the port, or change the left-side port in `docker-compose.yml`            |
| Jupyter opens without the notebook               | `docker compose up` was run from a different folder                                               |
| Windows asks to share the folder                 | Accept the request                                                                                |
| The kernel was restarted and the UI disappeared  | Run again starting from Mission 3                                                                 |
