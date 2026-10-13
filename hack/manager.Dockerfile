# Packages a manager binary built on the host (make image-local). Compiling
# inside a container needs a full module download and a cold build cache,
# which on a small WSL2 VM costs more memory than the whole test cluster.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
