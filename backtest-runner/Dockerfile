FROM continuumio/anaconda3:2024.02-1

ARG GITLAB_PYPI_TOKEN

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /work

# System dependencies (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# 1) Create conda environment from environment.yml
COPY environment.yml /tmp/environment.yml
RUN conda env create -n cryptam-dev -f /tmp/environment.yml \
 && conda clean -afy

# 2) Install private pip packages inside the SAME conda environment (GitLab PyPI)
RUN conda run -n cryptam-dev python -m pip install --upgrade pip \
 && conda run -n cryptam-dev pip install \
    --index-url "https://__token__:${GITLAB_PYPI_TOKEN}@gitlab.com/api/v4/projects/60135479/packages/pypi/simple" \
    --extra-index-url "https://__token__:${GITLAB_PYPI_TOKEN}@gitlab.com/api/v4/projects/62327534/packages/pypi/simple" \
    "cryptam==1.9.10a3" \
    "ee==3.5.3a4"

# 3) Install the HTTP server dependencies inside the SAME environment
#    python-multipart is required for FastAPI Form/File uploads
RUN conda run -n cryptam-dev pip install --no-cache-dir \
    fastapi uvicorn pydantic python-multipart

# 4) Copy server file
COPY server.py /work/server.py
COPY logfilesplit.py /work/logfilesplit.py
COPY transforms.py /work/transforms.py

EXPOSE 8000

# 5) Start uvicorn inside the conda environment
CMD ["conda", "run", "--no-capture-output", "-n", "cryptam-dev", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
