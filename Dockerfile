FROM python:3.8

ENV PYTHONUNBUFFERED 1
ENV SPACEONE_PORT 50051
ENV PKG_DIR /tmp/pkg
ENV SRC_DIR /tmp/src
ENV CONF_DIR /etc/spaceone
ENV LOG_DIR /var/log/spaceone
ENV EXTENSION_NAME extension
ENV EXTENSION_SRC_DIR /opt/spaceone


COPY pkg/*.txt ${PKG_DIR}/

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.1 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

RUN pip install --upgrade pip && \
    pip install --upgrade -r ${PKG_DIR}/pip_requirements.txt

RUN mkdir -p ${EXTENSION_SRC_DIR}/${EXTENSION_NAME} ${CONF_DIR} ${LOG_DIR}
RUN echo "__path__ = __import__('pkgutil').extend_path(__path__, __name__)" >> ${EXTENSION_SRC_DIR}/__init__.py
RUN echo "name = '${EXTENSION_NAME}'" >> ${EXTENSION_SRC_DIR}/${EXTENSION_NAME}/__init__.py

ARG CACHEBUST=1
RUN pip install --upgrade --pre spaceone-core spaceone-api

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}
RUN python3 setup.py install && \
    rm -rf /tmp/*

EXPOSE ${SPACEONE_PORT}

ENTRYPOINT ["spaceone"]
CMD ["grpc", "spaceone.inventory", "-m", "/opt"]