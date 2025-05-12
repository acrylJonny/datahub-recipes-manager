FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND noninteractive

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create a non-root user to run the application
RUN useradd -m appuser
RUN chown -R appuser:appuser /app
USER appuser

# Set up data directories with proper permissions
RUN mkdir -p /app/data /app/recipes /app/templates
VOLUME ["/app/data", "/app/recipes", "/app/templates"]

# Expose the application port
EXPOSE 8000

# Start the application with database initialization
CMD ["sh", "-c", "python scripts/upgrade_db.py && python web_ui/manage.py runserver 0.0.0.0:8000"] 