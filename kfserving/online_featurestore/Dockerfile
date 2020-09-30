FROM python:3.7-slim

COPY . .

# Update packages, pip and install kfserving
RUN apt-get update && pip install --upgrade pip
RUN pip install kfserving

# hsfs depends on twofish cryptography library which needs gcc.
# python:slim images does not contain gcc
RUN apt-get install -y --no-install-recommends -o APT::Keep-Downloaded-Packages=false gcc python3-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install hsfs \
    && apt-get purge -y --auto-remove gcc python3-dev

# Install other transformer module dependencies
RUN pip install -e .

ENTRYPOINT ["python", "-m", "online_fs_transformer"]
