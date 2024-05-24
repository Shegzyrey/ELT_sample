# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /usr/src/filler

# Copy the current directory contents into the container at /app
COPY . .

#RUN pip3 install --upgrade pip

#RUN pip3 install --no-cache-dir -r requirements.txt

RUN pip3 install -r requirements.txt
# Run app.py when the container launches
CMD ["python", "populate_oltp_table.py"]
#CMD ["python", "populate_table.py", "fill_data", "--count=10"]

#CMD ["flask", "fill_data", "--count=10"]