FROM astrocrpublic.azurecr.io/runtime:3.3-11
# Paste everything below UNDER the existing FROM line in your airflow\Dockerfile.
# Keep your own FROM line exactly as astro dev init wrote it.

# 1. Microsoft ODBC Driver 18 for SQL Server (pyodbc needs it on Linux too)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && . /etc/os-release \
 && curl -fsSL -o /tmp/packages-microsoft-prod.deb "https://packages.microsoft.com/config/debian/${VERSION_ID}/packages-microsoft-prod.deb" \
 && dpkg -i /tmp/packages-microsoft-prod.deb \
 && rm /tmp/packages-microsoft-prod.deb \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev \
 && rm -rf /var/lib/apt/lists/*
USER astro

# 2. dbt in its own virtual environment, so its packages cannot conflict with Airflow's
RUN python -m venv dbt_venv \
 && dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-snowflake

