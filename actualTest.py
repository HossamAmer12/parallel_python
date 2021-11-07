
from Queue import Queue


# 1. Read the file into batches and put into queue
from multiprocessing import Process, Queue
from threading import Thread

from itertools import islice

class Mulithreader():

    def __init__(self):
        self.processes = []
        self.queue = Queue()

    @staticmethod
    def _wrapper(func, queue, args, kwargs):
        ret = func(*args, **kwargs)
        queue.put(ret)

    def run(self, func, *args, **kwargs):
        args2 = [func, self.queue, args, kwargs]
        p = Process(target=self._wrapper, args=args2)
        self.processes.append(p)
        p.start()

    def wait(self):
        rets = []
        for p in self.processes:
            ret = self.queue.get()
            rets.append(ret)
        for p in self.processes:
            p.join()
        self.processes = []
        return rets


# more advanced: uses a worker pool and gets results in order, like Celery
class Batcher():

    CMD_JOB = 0
    CMD_KILL = 1

    def __init__(self, num_workers):
        self.jobs = []
        self.num_workers = num_workers

    @staticmethod
    def _worker(in_queue, out_queue):
        while True:
            # listen for new jobs
            cmd, index, job = in_queue.get()
            if cmd == Batcher.CMD_JOB:
                # process job, return result
                # job is a function
                func, args, kwargs = job
                ret = func(*args, **kwargs)
                # print("job:",  cmd, index, job)
                print("Args: ", args, kwargs, ret)
                out_queue.put((index, ret))
            elif cmd == Batcher.CMD_KILL:
                # time to stop
                return
            else:
                assert False

    def enqueue(self, func, *args, **kwargs):
        job = (func, args, kwargs)
        self.jobs.append(job)

    def process(self, num_workers=None):
        if num_workers is None:
            num_workers = self.num_workers
        # spawn workers
        in_queue, out_queue = Queue(), Queue()
        workers = []
        for _ in range(num_workers):
            # p = Process(target=self._worker, args=(in_queue, out_queue))
            p = Thread(target=self._worker, args=(in_queue, out_queue))
            workers.append(p)
            p.start()

        # put jobs into queue
        job_idx = 0
        for start in range(0, len(self.jobs), num_workers):
            for job in self.jobs[start: start + num_workers]:
            	print("Input queue: ", job_idx, job)
                in_queue.put((Batcher.CMD_JOB, job_idx, job))
                job_idx += 1

        # get results from workers
        results = [None] * len(self.jobs)
        for _ in range(len(self.jobs)):
            res_idx, res = out_queue.get()
            assert results[res_idx] == None
            results[res_idx] = res

        # stop workers
        for _ in range(num_workers):
            in_queue.put((Batcher.CMD_KILL, None, None))
        for i in range(num_workers):
            workers[i].join()

        return results


def custom_translate(myList):
    return str(myList)

# tester/examples
if __name__ == "__main__":

    mp2 = Batcher(num_workers=2)

    batch_size = 2
    # total length over batch size
    num_jobs = 20 / batch_size

    filename = "bla.txt"
    with open(filename, 'rb') as f:
        for _batch in iter(lambda: tuple(islice(f, batch_size)), ()):
            print(_batch)
            mp2.enqueue(custom_translate, map(int, _batch))
            # mp2.enqueue(sum, map(int,_batch))
            
    ret = mp2.process()
    print(ret)
    # assert len(ret) == num_jobs and all(r == 15 + i for i, r in enumerate(ret))

    

# 2. Process the batch in one thread

# 3. Get the results in out queue
