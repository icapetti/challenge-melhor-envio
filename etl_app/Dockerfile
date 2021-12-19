FROM python:3.9

# Set the working directory
WORKDIR /etl_app_code

ENV MYSQL_ROOT_PASSWORD=rootpass
 
# Copy the project source code from the local host to the filesystem of the container at the working directory.
COPY . .
# Install requirements.txt.
RUN pip3 install -r requirements.txt
 
# Run the crawler when the container launches.
CMD ["python3", "./etl_logs.py"]