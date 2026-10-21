#!/usr/bin/env python3
"""Run discovery fault/lifecycle checks against the broker using the built product's libraries."""
import os
from pathlib import Path
import subprocess
import sys
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
products = REPO / "org.titou10.jtb.product/target/products/org.titou10.jtb.product"
plugins = next(products.glob("**/plugins"))
target = HERE / "target/broker-discovery-check"
target.mkdir(parents=True, exist_ok=True)

# Use the actual packaged classes and embedded libraries; no extra Maven dependencies or test stubs.
classpath = [str(target)] + [str(plugin) for plugin in sorted(plugins.glob("*.jar"))]
for plugin in sorted(plugins.glob("*.jar")):
    if plugin.name.startswith(("org.titou10.jtb.core_", "org.titou10.jtb.qm.activemq_")):
        with zipfile.ZipFile(plugin) as archive:
            for entry in archive.namelist():
                if entry.startswith("lib/") and entry.endswith(".jar"):
                    path = target / plugin.stem / entry
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.read(entry))
                    classpath.append(str(path))

cp = os.pathsep.join(classpath)
compiled = subprocess.run(["javac", "-cp", cp, "-d", str(target), str(HERE / "BrokerDiscoveryCheck.java")])
if compiled.returncode:
    sys.exit(compiled.returncode)
try:
    result = subprocess.run(["java", "-Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider",
                             "-cp", cp, "BrokerDiscoveryCheck"], timeout=90)
    sys.exit(result.returncode)
except subprocess.TimeoutExpired:
    sys.exit("Broker discovery checks did not finish within 90 seconds")
