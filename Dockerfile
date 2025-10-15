FROM cloudforet/python-core:2.0.96
ARG PACKAGE_VERSION
ENV PYTHONUNBUFFERED 1
ENV SPACEONE_PORT 50051
ENV SRC_DIR /tmp/src
ENV CONF_DIR /etc/spaceone
ENV LOG_DIR /var/log/spaceone
ENV PACKAGE_VERSION=$PACKAGE_VERSION

COPY pkg/pip_requirements.txt pip_requirements.txt

RUN pip install --upgrade pip && \
    pip install --upgrade -r pip_requirements.txt

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}

RUN python3 setup.py install && rm -rf /tmp/*

RUN pip install --upgrade spaceone-api==2.0.266

EXPOSE ${SPACEONE_PORT}

ENTRYPOINT ["spaceone"]
CMD ["run", "grpc-server", "spaceone.inventory", "-m", "/opt"]
