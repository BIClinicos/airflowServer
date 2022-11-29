FROM apache/airflow:2.0.1-python3.8
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         unixodbc-dev gcc curl libxml2-dev libxslt1-dev zlib1g-dev g++
RUN apt-get install -y gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN apt-get install -y libgssapi-krb5-2
COPY requirements.txt requirements.txt
COPY airflow.cfg airflow.cfg
RUN pip install --upgrade pip==22.1.2
RUN pip install -r requirements.txt
USER airflow
# RUN pip install apache-airflow-providers-microsoft-azure==1.2.0rc1
# RUN pip install apache-airflow-providers-microsoft-mssql[odbc]