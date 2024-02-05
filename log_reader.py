import subprocess
import threading
import time
# from log_to_doc_converter import LogToDocConverter  # Uncomment if used

class LogReader:
    def __init__(self, service, queue):
        self.service = service
        self.queue = queue
        # self.converter = LogToDocConverter()  # Uncomment if used
        self.stop_event = threading.Event()

    def service_exists(self):
        check_command = ["journalctl", "-u", self.service, "-n", "1"]
        check_process = subprocess.Popen(check_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, _ = check_process.communicate()
        if check_process.returncode != 0 or "No entries" in stdout:
            return False
        return True

    def start(self):
        if not self.service_exists():
            print(f"Service '{self.service}' does not exist or has no logs.")
            return
        self.thread = threading.Thread(target=self.read_logs)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()

    def read_logs(self):
        while not self.stop_event.is_set():
            command = ["journalctl", "-u", self.service, "-n", "100", "-r" , "-o", "json"]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            try:
                for _ in range(100):  # Read 100 lines or until process.stdout is empty
                    if self.stop_event.is_set():
                        break
                    line = process.stdout.readline()
                    if not line:
                        break
                    # json_doc = self.converter.convert(line)  # Uncomment if conversion is used
                    json_doc = line  # Use directly if no conversion is needed
                    self.queue.put(json_doc)
            finally:
                process.terminate()

            # Wait for 30 seconds before continuing
            time.sleep(30)

            # Clear the queue if not empty
            while not self.queue.empty():
                self.queue.get()  # Remove items from the queue
            print (f"\n\n\n queue is cleared \n\n\n")
