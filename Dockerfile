FROM ubuntu

ENV DIR_PATH=/app
ENV DATA_PATH=/storage

RUN mkdir $DIR_PATH && mkdir $DATA_PATH

COPY . $DIR_PATH
WORKDIR $DIR_PATH

RUN apt-get update && \
    apt-get install -y python3.8 && \
    apt-get install -y python3-pip && \
    apt-get install -y git && \
    apt-get install -y gcc

#RUN python -m pip install --upgrade pip
RUN pip install PyQt5 pyqtgraph cython numpy==1.20.3
RUN git clone https://github.com/MikkoArtik/SeisCore.git
RUN cd /app/SeisCore && git checkout newVersion && \
    cd seiscore/binaryfile/resampling && \
    python3.8 setup.py build_ext --inplace
RUN cd /app/SeisCore && pip install -e . && rm -r /app/SeisCore

WORKDIR $DIR_PATH/ui

ENTRYPOINT ["bash"]
