#!/usr/bin/env python3
"""Interleaved A/B benchmark of two gnet bench-server binaries, driven by wrk (Linux).

Sandbox noise (CPU frequency, noisy neighbours, scheduler luck) is larger than most
micro-optimisations, so runs of A and B are interleaved (A B A B ...) and compared by
their medians; the server's CPU time per request is read from /proc, which is far less
noisy than RPS when client and server share cores.

  # build the two servers (bench/server is a nested module, build from inside it)
  (cd bench/server && go build -o /tmp/srv_new .)       # the tree under test
  # ... and the same from the baseline tree -> /tmp/srv_old

  # keep-alive HTTP, 64 connections
  python3 bench/scripts/ab_wrk.py --a /tmp/srv_old --b /tmp/srv_new
  # pipelined (16 requests per write) - the case write coalescing is about
  python3 bench/scripts/ab_wrk.py --a /tmp/srv_old --b /tmp/srv_new --depth 16
  # connection churn
  python3 bench/scripts/ab_wrk.py --a /tmp/srv_old --b /tmp/srv_new --close
  # same binary, one option flipped (flags only apply to B here)
  python3 bench/scripts/ab_wrk.py --a /tmp/srv_new --b /tmp/srv_new --b-args="-noyield"
"""
import argparse, os, re, signal, socket, statistics, subprocess, sys, time

CLK = os.sysconf("SC_CLK_TCK")
HERE = os.path.dirname(os.path.abspath(__file__))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        rest = f.read().rsplit(")", 1)[1].split()
    return (int(rest[11]) + int(rest[12])) / CLK  # utime + stime


def run_once(binary, args, a):
    port = free_port()  # a fresh port per run: TIME_WAIT from a churn run must not block the next
    env = dict(os.environ, GOMAXPROCS=str(a.gomaxprocs))
    srv = subprocess.Popen([binary, "-addr", f"tcp4://127.0.0.1:{port}", *a.server_args.split(), *args.split()],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError(f"{binary} did not start")
        time.sleep(0.3)
        cmd = ["wrk", f"-t{a.threads}", f"-c{a.conns}", f"-d{a.dur}s"]
        if a.close:
            cmd += ["-H", "Connection: close"]
        if a.depth > 1:
            cmd += ["-s", os.path.join(HERE, "..", "pipeline.lua")]
        cmd.append(f"http://127.0.0.1:{port}/bench")
        if a.depth > 1:
            cmd += ["--", str(a.depth)]
        c0 = cpu_seconds(srv.pid)
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=a.dur + 30).stdout
        c1 = cpu_seconds(srv.pid)
        rps = float(re.search(r"Requests/sec:\s+([\d.]+)", out).group(1))
        total = int(re.search(r"(\d+) requests in", out).group(1))
        return rps, (c1 - c0) * 1e6 / total  # RPS, server CPU microseconds per request
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(5)
        except subprocess.TimeoutExpired:
            srv.kill()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--a", required=True, help="baseline server binary")
    p.add_argument("--b", required=True, help="server binary under test")
    p.add_argument("--a-args", default="", help='extra flags for A only, written with "=": --a-args="-noyield"')
    p.add_argument("--b-args", default="", help='extra flags for B only, written with "=": --b-args="-noyield"')
    p.add_argument("--server-args", default="-mode http -loops 1", help="flags for both servers")
    p.add_argument("--conns", type=int, default=64)
    p.add_argument("--threads", type=int, default=1, help="wrk threads")
    p.add_argument("--depth", type=int, default=1, help="pipelined requests per write")
    p.add_argument("--close", action="store_true", help="Connection: close (one request per connection)")
    p.add_argument("--reps", type=int, default=4)
    p.add_argument("--dur", type=int, default=5, help="seconds per run")
    p.add_argument("--gomaxprocs", type=int, default=2, help="GOMAXPROCS of the server (>1 avoids a single-P artefact)")
    a = p.parse_args()

    res = {"A": [], "B": []}
    for i in range(a.reps):
        for name, binary, args in (("A", a.a, a.a_args), ("B", a.b, a.b_args)):
            rps, cpu = run_once(binary, args, a)
            res[name].append((rps, cpu))
            print(f"  run {i + 1} {name}: {rps:>10.0f} req/s   {cpu:6.3f} us server CPU/req", file=sys.stderr)
    med = lambda xs, k: statistics.median(x[k] for x in xs)
    ra, rb, ca, cb = med(res["A"], 0), med(res["B"], 0), med(res["A"], 1), med(res["B"], 1)
    print(f"A: {ra:,.0f} req/s  {ca:.3f} us/req   B: {rb:,.0f} req/s  {cb:.3f} us/req   "
          f"=> RPS {100 * (rb / ra - 1):+.1f}%   CPU/req {100 * (cb / ca - 1):+.1f}%")


if __name__ == "__main__":
    main()
