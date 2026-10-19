# The linearizability checker, packaged like the manager: built on the
# host (make image-checker) and copied into distroless.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY checker /checker
USER 65532:65532
ENTRYPOINT ["/checker"]
