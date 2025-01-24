FROM continuumio/miniconda3:24.1.2-0 as base
ARG pipeline

COPY "./stactools_pipelines/pipelines/${pipeline}/*" ./
COPY "./stactools_pipelines/cognito/*" "./stactools_pipelines/cognito/"
COPY "./stactools_pipelines/pipelines/${pipeline}/collection.py" "./app.py"
RUN conda update conda && \
    conda env update -f environment.yml -n base && \
    conda clean -afy


FROM base as dependencies

ENV PATH="/opt/conda/bin:$PATH"
COPY --from=base /opt/conda /opt/conda
COPY lambda_setup.py ./setup.py
RUN apt-get -y -q update \
    && apt-get -y -q install build-essential \
    && rm -rf /var/lib/apt/lists/
RUN pip install .


FROM dependencies as builder

ENV PATH="/opt/conda/bin:$PATH"
COPY --from=base /opt/conda /opt/conda
ENTRYPOINT [ "/opt/conda/bin/python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]
