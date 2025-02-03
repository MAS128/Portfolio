# Base Python slim image
FROM python:3.12.6-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set label
LABEL app="llama_tagger"

# Set the working directory
WORKDIR /app

# Install system dependencies including ping (iputils-ping)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cron \
        supervisor \
        netcat-openbsd \
        gcc \
        libpq-dev \
        iputils-ping \
	curl \
    && rm -rf /var/lib/apt/lists/*

# Create log directories
RUN mkdir -p /var/log/supervisor /var/log/cron /var/log/tagger

# Cronjobs
#COPY cron.jobs /etc/cron.d/cron.jobs
#RUN chmod 0644 /etc/cron.d/cron.jobs
#RUN crontab /etc/cron.d/cron.jobs

# Copy requirements file
COPY requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the entire application to the working directory
COPY . .

# Copy the Supervisor config file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Start Supervisor, which will manage cron and tagger
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
