FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir pyyaml

# Copy source code
COPY src/ /app/src/
COPY examples/ /app/examples/
COPY config/ /app/config/

# Set Python path
ENV PYTHONPATH=/app/src

# Expose port
EXPOSE 8080

# Default command
CMD ["python", "examples/api_server_config.py"]
