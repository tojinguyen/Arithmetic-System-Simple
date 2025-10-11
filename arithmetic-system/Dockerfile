# Use official Python image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set the default command to run the main application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

