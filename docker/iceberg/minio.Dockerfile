FROM debian:bookworm-slim

ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && arch="$(dpkg --print-architecture)" \
    && case "$arch" in \
         amd64) cpu=amd64 ;; \
         arm64) cpu=arm64 ;; \
         *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
       esac \
    && curl -fsSL --retry 3 --retry-all-errors -o /usr/local/bin/minio \
         "https://github.com/minio/minio/releases/download/${MINIO_RELEASE}/minio.linux-${cpu}.${MINIO_RELEASE}" \
    && curl -fsSL --retry 3 --retry-all-errors -o /usr/local/bin/mc \
         "https://github.com/minio/mc/releases/download/${MC_RELEASE}/mc.linux-${cpu}.${MC_RELEASE}" \
    && chmod 755 /usr/local/bin/minio /usr/local/bin/mc \
    && minio --version \
    && mc --version \
    && mc ready --help >/dev/null \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
