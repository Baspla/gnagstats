# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Install locales package and generate German locale
RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/*
RUN sed -i '/de_DE.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

# Set environment variables for locale
ENV LANG de_DE.UTF-8
ENV LANGUAGE de_DE:de
ENV LC_ALL de_DE.UTF-8

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Specify the command to run your Python program
CMD ["python", "main.py"]
