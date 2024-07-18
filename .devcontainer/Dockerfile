FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

ENV PYTHONUNBUFFERED 1

# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
COPY requirements.txt /tmp/pip-tmp/
RUN grep -v 'connectorx==0.3.3' /tmp/pip-tmp/requirements.txt > /tmp/pip-tmp/down_graded_connectorx_requirements.txt \
   && pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/down_graded_connectorx_requirements.txt \
   && pip3 install connectorx==0.2.3 \
   && rm -rf /tmp/pip-tmp
# [Without skipping connectorx installation]
# RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
#    && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends graphviz
