from threading import Thread
from queue import Queue
import struct

class BinWriteQueue:
    """Manages a dedicated thread for writing adjacency data to a binary file."""
    
    def __init__(self, filename="adjacency.bin"):
        self.filename = filename
        self.write_queue = Queue()
        self.writer_thread = None
    
    def _writer_thread(self):
        """Dedicated thread that writes adjacency data to file."""
        with open(self.filename, "ab") as f:
            while True:
                event = self.write_queue.get()
                if event is None:  # Sentinel value to stop the thread
                    break
                src_id, dest_ids = event
                f.write(struct.pack("<II", src_id, len(dest_ids)))
                if dest_ids:
                    f.write(struct.pack("<" + "I"*len(dest_ids), *dest_ids))
                self.write_queue.task_done()
    
    def start(self):
        self.writer_thread = Thread(target=self._writer_thread, daemon=False)
        self.writer_thread.start()
    
    def send(self, src_id, dest_ids):
        self.write_queue.put((src_id, dest_ids))
    
    def shutdown(self):
        self.write_queue.join()
        self.write_queue.put(None)
        if self.writer_thread:
            self.writer_thread.join()
