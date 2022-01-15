FROM apache/airflow:2.2.3-python3.8
RUN pip3 install --upgrade pip
#RUN pip3 install --no-cache-dir lxml
#RUN pip3 install newspaper3k
#COPY requirements.txt .
#RUN pip3 install -r requirements.txt


# t4
#USER root
#RUN sudo add-apt-repository ppa:rock-core/qt4
#RUN sudo apt update
#RUN sudo apt-get install qt4-dev-tools libqt4-dev libqt4-core libqt4-gui
#USER airflow
