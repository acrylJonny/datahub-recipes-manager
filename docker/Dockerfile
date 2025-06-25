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
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

# Copy the application code
COPY . .

# Create a non-root user to run the application
RUN useradd -m appuser
RUN chown -R appuser:appuser /app
USER appuser

# Set up data directories with proper permissions
RUN mkdir -p /app/data /app/recipes /app/templates /app/metadata-manager
VOLUME ["/app/data", "/app/recipes", "/app/templates", "/app/metadata-manager"]

# Expose the application port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health/ || exit 1

# Make the startup script executable and use it
RUN chmod +x run_web_ui.sh

# Start the application using the unified startup script
CMD ["./run_web_ui.sh", "production", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"] 