import subprocess
import sys
import time

def main():
    num_workers = 6 # Matches your WSL core allocation
    bootstrap = "localhost:9092"
    topic = "quickstart-events"
    
    processes = []
    print(f"🚀 Spawning {num_workers} YOLO workers...")

    for i in range(num_workers):
        # Starts worker_logic.py (see script #3)
        p = subprocess.Popen([sys.executable, 'worker_logic.py', str(i), bootstrap, topic])
        processes.append(p)

    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nStopping workers...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()