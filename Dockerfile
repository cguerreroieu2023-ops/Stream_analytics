FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir fastavro

# Copy generator code and schemas
COPY generator/ ./generator/
COPY sample_data/ ./sample_data/

# Default output directory
RUN mkdir -p /app/output

ENTRYPOINT ["python", "generator/generator.py"]
CMD ["--num-orders", "200", "--output-dir", "/app/output"]
