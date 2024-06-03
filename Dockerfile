# Use an official Python runtime as a parent image
FROM python:3.10.5

# Set the working directory in the container to /app
WORKDIR /app

# Add metadata to the image to describe that the container is listening on the specified port at runtime.
EXPOSE 8000

# Copy the current directory contents into the container at /app
COPY . /app

# Copy the contents of the local /tmp folder into the /tmp volume on the container
COPY ./tmp /tmp

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

# Create a volume for tmp
VOLUME /tmp

# Run the application
CMD ["python", "main.py"]