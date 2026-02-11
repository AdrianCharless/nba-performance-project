import subprocess
import sys

STEPS = [
    [sys.executable, "src/ingest.py"],
    [sys.executable, "src/features.py"],
    [sys.executable, "src/train_model.py"],
    [sys.executable, "src/detect_anomalies.py"],
]

if __name__ == "__main__":
    for cmd in STEPS:
        print("\nRunning:", " ".join(cmd))
        r = subprocess.run(cmd)
        if r.returncode != 0:
            sys.exit(r.returncode)
    print("\nPipeline complete")
