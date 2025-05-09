FROM --platform=linux/amd64 gcr.io/distroless/base-debian11
ARG USER_UID=10001
ARG USER_GID=10001
USER ${USER_UID}:${USER_GID}
COPY otelcontribcol /
EXPOSE 4317/tcp 55680/tcp 55678/tcp 55679/tcp 8086/tcp
CMD ["/otelcontribcol"] 
