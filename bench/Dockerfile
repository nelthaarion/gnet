# One image containing every build we want to compare, built from the two
# source trees that are part of the build context:
#
#   baseline/  the engine exactly as it is committed today (pre-improvement)
#   current/   the engine with the hot-path improvements applied
#
# and, for each of them, two compilations:
#
#   *_default  no build tags        -- map[int]*conn, fd-keyed epoll dispatch
#   *_opt      -tags gc_opt,poll_opt -- conn matrix, callback-pointer dispatch

FROM golang:1.27-alpine AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-mod=mod

# The load generator never imports gnet, so it is built once and shared.
COPY current/bench/client/go.mod current/bench/client/main.go /src/client/
RUN cd /src/client && go build -o /out/loadgen .

# ---- baseline engine ----
COPY baseline/ /src/baseline/
RUN cd /src/baseline \
 && go build -o /out/baseline_default ./bench/server \
 && go build -tags "gc_opt poll_opt" -o /out/baseline_opt ./bench/server

# ---- current engine (improvements applied) ----
COPY current/ /src/current/
RUN cd /src/current \
 && go build -o /out/current_default ./bench/server \
 && go build -tags "gc_opt poll_opt" -o /out/current_opt ./bench/server

FROM alpine:3.21
WORKDIR /bench
COPY --from=build /out/ /bench/

# Which server to run is chosen by the BENCH_SERVER env var, e.g.
#   docker run -e BENCH_SERVER=current_opt gnet-bench
CMD ["sh", "-c", "/bench/${BENCH_SERVER:-baseline_default}"]
