import queue
import threading
import logging as log

logging = log.getLogger(__name__)


class ThreadQueueManager:
    def __init__(self, num_workers, processor: 'BaseProcessor'):
        self.terminated = False
        self.remainder = 0
        self.num_workers = num_workers
        self.count = 0
        self.processor = processor
        self.queue = queue.Queue()


    def start(self):
        self.workers = [threading.Thread(target=self.worker) for _ in range(self.num_workers)]
        for worker in self.workers:
            worker.daemon = True
            worker.start()

    def worker(self):
        max_wait_count = 150
        max_wait_time = 1
        wait_count = 0

        while StatusCode.RUNNING == self.processor.get_current_status():

            # we do not want to block on this,
            try:
                function = self.queue.get(timeout=max_wait_time)
            except queue.Empty:
                if wait_count >= max_wait_count:
                    total_wait_time = max_wait_time * max_wait_count
                    logging.info(f'max wait time expired, waited a total of {total_wait_time}, count: {wait_count}/{max_wait_count}')
                    self.terminated = True

                wait_count += 1
                continue

            # invoke the function
            try:
                function()
                self.remainder -= 1
                logging.info(f'completed worker task {function}, remainder: {self.remainder}')
            except Exception as e:
                logging.error(f'severe exception on worker function {e} for function: {function}')
                # raise e
            finally:
                self.queue.task_done()

    def add_to_queue(self, item):
        logging.info(f'added worker task {item} to queue at position {self.count}')
        self.count += 1
        self.remainder += 1
        self.queue.put(item)

    def wait_for_completion(self):
        self.queue.join()

    def stop_all_workers(self):
        self.terminated = True