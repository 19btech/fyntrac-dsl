FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy the backend source code
COPY backend/ ./backend/


# Set the python path so the backend module can be imported properly
ENV PYTHONPATH=/app
ENV HOST=0.0.0.0
ENV PORT=8000

# Expose the application port
EXPOSE 8000

CMD ["python", "-m", "backend.server"]
