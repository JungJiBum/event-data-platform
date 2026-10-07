"""Run from the repository root: python3 tests/grafana_checks.py.

Creates a disposable Grafana with empty storage, then removes it and its volumes.
"""
import json
from pathlib import Path
import subprocess
import time
import unittest
from uuid import uuid4


class GrafanaChecks(unittest.TestCase):
    def test_provisioning_with_empty_storage(self):
        root = Path(__file__).resolve().parents[1]
        name = "portfolio-grafana-test-" + uuid4().hex[:12]
        image = subprocess.check_output([
            "docker", "inspect", "monitoring-grafana-1", "--format", "{{.Image}}"
        ], text=True).strip()
        subprocess.run([
            "docker", "run", "-d", "--name", name,
            "--network", "event-platform-network",
            "-e", "GF_AUTH_ANONYMOUS_ENABLED=true",
            "-v", f"{root}/monitoring/provisioning:/etc/grafana/provisioning:ro",
            "-v", f"{root}/monitoring/dashboards:/etc/grafana/dashboards:ro",
            image,
        ], check=True, stdout=subprocess.DEVNULL)
        try:
            def get(path):
                p = subprocess.run([
                    "docker", "exec", name, "wget", "-qO-", "http://localhost:3000" + path
                ], capture_output=True, text=True)
                if p.returncode:
                    raise RuntimeError(p.stderr)
                return json.loads(p.stdout)
            deadline = time.monotonic() + 45
            while True:
                try:
                    self.assertEqual(get("/api/health")["database"], "ok")
                    break
                except (RuntimeError, json.JSONDecodeError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(1)
            dashboards = get("/api/search")
            self.assertTrue(any(d["uid"] == "event-data-platform-operations" for d in dashboards))
            dashboard = get("/api/dashboards/uid/event-data-platform-operations")["dashboard"]
            self.assertEqual(len(dashboard["panels"]), 6)
            source = get("/api/datasources/uid/dg0igerm6lfk0a")
            self.assertEqual(source["url"], "http://prometheus:9090")
            self.assertEqual(source["type"], "prometheus")
        finally:
            subprocess.run(["docker", "rm", "-fv", name], check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
