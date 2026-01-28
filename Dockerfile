FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY database.py .

# Create non-root user
RUN useradd -m -u 1000 botuser
USER botuser

# Create tmp directory for database
RUN mkdir -p /tmp

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV RENDER=true
ENV PORT=8080
ENV WEB_SERVER_HOST=0.0.0.0

# Expose port
EXPOSE 8080

# Run the bot
CMD ["python", "main.py"]