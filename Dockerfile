FROM apache/airflow:2.3.0-python3.8
RUN pip3 install --upgrade pip
#RUN pip3 install --no-cache-dir lxml
RUN pip3 install newspaper3k
RUN pip install apache-airflow-providers-amazon
#COPY requirements.txt .
#RUN pip3 install -r requirements.txt


# t4
#USER root
#RUN sudo add-apt-repository ppa:rock-core/qt4
#RUN sudo apt update
#RUN sudo apt-get install qt4-dev-tools libqt4-dev libqt4-core libqt4-gui
#USER airflow


# FROM python:3
# WORKDIR /usr/src/app
# COPY web_py_requirements.txt ./
# RUN pip install --no-cache-dir -r web_py_requirements.txt
# #COPY . .
# #CMD [ "python", "./your-daemon-or-script.py" ]