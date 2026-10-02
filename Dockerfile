# Pinned by digest (the multi-architecture index of python:3.11-bookworm on 2026-10-01), so a
# rebuild starts from the same base image; to move to a newer one, replace the digest. The apt
# packages, uwsgi and the dependencies of requirements.txt are resolved at build time (only the
# requirements themselves are pinned, by version), so two builds can still differ in those.
FROM python:3.11-bookworm@sha256:8d9c82537acb2273a53b818d1ca55a3fd4eea6ab2b92f395998b7a5f49fa3cf3

# Development
RUN apt-get update
RUN apt-get install webp -y

# This specific version of ImageMagick is required for compatibility with Wand.
# It is fetched from the GitHub release tag (the imagemagick.org download hosts have moved and
# refuse connections at times) and checked against a pinned SHA-256, so a changed or tampered
# archive fails the build instead of being compiled. The source tree is removed in the same layer
# so it does not add ~100 MB to the image.
ARG IMAGEMAGICK_VERSION=6.9.10-90
ARG IMAGEMAGICK_SHA256=b7b2335b05e75c80c1f472c8662c49375904e378ca79d098909078d8a43f04b7
RUN wget -q -O /tmp/imagemagick.tar.gz \
        "https://github.com/ImageMagick/ImageMagick6/archive/refs/tags/${IMAGEMAGICK_VERSION}.tar.gz" && \
    echo "${IMAGEMAGICK_SHA256}  /tmp/imagemagick.tar.gz" | sha256sum -c - && \
    mkdir /tmp/imagemagick && \
    tar -xzf /tmp/imagemagick.tar.gz -C /tmp/imagemagick --strip-components=1 && \
    cd /tmp/imagemagick && \
    ./configure --with-webp=yes && \
    make -j"$(nproc)" && \
    make install && \
    cd / && \
    rm -rf /tmp/imagemagick /tmp/imagemagick.tar.gz && \
    ldconfig /usr/local/lib

RUN pip install uwsgi uwsgitop

WORKDIR /prism
COPY requirements.txt ./
RUN pip install -U pip && pip install -r requirements.txt
COPY prism.uwsgi.ini ./
COPY prism ./prism/

# Expose HTTP port
EXPOSE 8000
# Expose uwsgi port
EXPOSE 3001

# These uwsgi options are set here as environment variables so they can be overridden later
ENV UWSGI_PROCESSES=2
ENV UWSGI_THREADS=2

CMD ["uwsgi", "prism.uwsgi.ini"]
