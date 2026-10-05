# Use a slim Python base image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy dependency file first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your whole project
COPY src .

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=1

# RUN mkdir -p /app/data

# Default command: keep container alive
# CMD ["tail", "-f", "/dev/null"]
CMD ["python", "main.py"]