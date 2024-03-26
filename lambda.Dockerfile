FROM python:3.11 as builder-pip
ARG pipeline
COPY "./stactools_pipelines/pipelines/${pipeline}/requirements.txt" .
RUN pip install -r requirements.txt --target /tmp/site-packages

FROM continuumio/miniconda3:24.1.2-0 as builder-conda
ARG pipeline
COPY "./stactools_pipelines/pipelines/${pipeline}/environment.yml" .
RUN conda env update -f environment.yml -n base && \
    conda clean -afy

FROM public.ecr.aws/lambda/python:3.11
ARG pipeline
COPY "./stactools_pipelines/cognito/*" "./stactools_pipelines/cognito/"
COPY lambda_setup.py ./setup.py
RUN pip install .

# ENV CURL_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.trust.crt 
# /var/task/certifi/cacert.pem
# /etc/ssl/certs/ca-bundle.crt

COPY --from=builder-pip /tmp/site-packages ${LAMBDA_TASK_ROOT}
COPY --from=builder-conda /opt/conda/lib /var/lang/lib
COPY --from=builder-conda /opt/conda/lib/python3.11/site-packages ${LAMBDA_TASK_ROOT}
COPY "./stactools_pipelines/pipelines/${pipeline}/*" "${LAMBDA_TASK_ROOT}/"
RUN rm -f /etc/ssl/certs/ca-bundle.crt && \
    pip install --upgrade --force-reinstall certifi && \
    update-ca-trust force-enable && \
    update-ca-trust extract
CMD [ "app.handler" ]
