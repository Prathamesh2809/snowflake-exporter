# Use the official Python 3.10 image from Docker Hub
FROM python:3.10-slim

# Set environment variables for non-interactive installations
ENV PYTHONUNBUFFERED 1

# Set working directory inside the container
WORKDIR /app

# Copy requirements file into the container
COPY app/requirements.txt /app/

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application files into the container
COPY app/. /app/

# Expose the port the exporter will run on
EXPOSE 8000

# Command to run the Snowflake Prometheus Exporter
CMD ["python", "exporter.py"]