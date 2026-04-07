FROM python:3.11-slim

WORKDIR /app

# System deps for lxml, trafilatura, sentence-transformers
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libxml2-dev libxslt-dev curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 -s /bin/sh newsprism

# ── Dependency layer (cached unless pyproject.toml changes) ────────────────
# Parse deps with stdlib tomllib (Python 3.11+) and install them first.
# This layer is only invalidated when pyproject.toml changes — not on every
# source edit, keeping iterative rebuilds fast.
COPY pyproject.toml .
COPY README.md .
# Parse deps from pyproject.toml and install them (single-line for Docker parser compatibility)
RUN python3 -c "import tomllib,subprocess,sys; d=tomllib.load(open('pyproject.toml','rb')); subprocess.run([sys.executable,'-m','pip','install','--no-cache-dir']+d['project']['dependencies'],check=True)"

# ── Source layer (re-runs only when source changes) ────────────────────────
COPY newsprism/ newsprism/
COPY config/ config/
COPY templates/ templates/

# Install the package itself (deps already satisfied above; --no-deps skips
# re-resolving the full dep tree so this step stays fast).
RUN pip install --no-cache-dir --no-deps -e .

# Data + output dirs (bind-mounted as volumes in prod; created here for local use)
RUN mkdir -p data output .cache/huggingface && chown -R newsprism:newsprism /app

USER newsprism

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
# HF model cache — keep path in sync with hf_cache volume mount in docker-compose.yml.
# The sentence-transformers model (~400 MB) downloads on first container start
# and is persisted in the named volume; subsequent starts skip the download.
ENV HF_HOME=/app/.cache/huggingface

CMD ["python", "-m", "newsprism", "run"]
