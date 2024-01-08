FROM python:3.10-slim as base
WORKDIR /app

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VERSION=1.6.1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_NO_INTERACTION=1

FROM base as builder
RUN python -m venv .venv
ENV PATH=/app/.venv/bin:$PATH

COPY requirements.txt .
RUN pip install -r requirements.txt

FROM base as release
COPY --from=builder /app/.venv ./.venv/
ENV PATH=/app/.venv/bin:$PATH

COPY pipelines/ ./pipelines/
COPY algo_features/ ./algo_features/

ENV PYTHONPATH="${PYTHONPATH}:/pipelines"
