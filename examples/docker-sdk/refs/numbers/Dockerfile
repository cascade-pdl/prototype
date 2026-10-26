# The base is pinned by digest: cascade names the built image by a hash of this
# directory, and a moving tag would build different images under the same name.
FROM python:3.13-slim@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962

# cascade's own source, which cascade passes to the build as the named context
# `cascade` (it is not on a package index)
COPY --from=cascade . /src/cascade
RUN pip install --no-cache-dir /src/cascade

COPY entrypoint.py /app/entrypoint.py
ENTRYPOINT ["python", "/app/entrypoint.py"]
