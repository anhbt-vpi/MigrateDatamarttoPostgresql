FROM deepnote/python:3.9
# UPDATE APT-GET
RUN apt-get update

# PYODBC DEPENDENCES
RUN apt-get install -y tdsodbc unixodbc-dev
RUN apt install unixodbc -y
RUN apt-get clean -y

# UPGRADE pip3
RUN pip3 install --upgrade pip

# DEPENDECES FOR DOWNLOAD ODBC DRIVER
RUN apt-get install apt-transport-https
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update


# INSTALL ODBC DRIVER
RUN ACCEPT_EULA=Y apt-get install msodbcsql18 --assume-yes

# CONFIGURE ENV FOR /bin/bash TO USE MSODBCSQL17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
# Cài đặt các gói cần thiết để sử dụng psycopg2
RUN apt-get update && \
    apt-get install -y python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt psycopg2
RUN pip install psycopg2-binary
RUN pip install pyodbc
RUN pip install sqlalchemy
RUN pip install retrying

# Set the working directory to /app
WORKDIR /app

# Copy the main.py file into the container
COPY main.py .

# Run main.py when the container launches
CMD ["python", "main.py"]