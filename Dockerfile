# Uses a lightweight Python base image
FROM python:3.10-slim

# Setting the working directory in the container
WORKDIR /app

# Copies the requirements file and installing dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copying the application code
COPY main.py .

# Command to run the application
CMD ["python", "main.py"]
