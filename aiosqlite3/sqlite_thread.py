
"""
thread
"""

from threading import Thread
from queue import Empty


class SqliteThread(Thread):
    """
    sqlite thread
    """
    def __init__(self, tx_queue, rx_queue, tx_event, rx_event):
        super(SqliteThread, self).__init__()
        self._tx_event = tx_event
        self._rx_event = rx_event
        self._tx_queue = tx_queue
        self._rx_queue = rx_queue
        self._stoped = False

    def run(self):
        """
        执行任务
        """
        while not self._stoped:
            self._tx_event.wait()
            self._tx_event.clear()
            try:
                func = self._tx_queue.get_nowait()
                if isinstance(func, str):
                    self._stoped = True
                    self._rx_queue.put('closed')
                    self.notice()
                    break
            except Empty:
                # pragma: no cover
                continue
            try:
                result = func()
                self._rx_queue.put(result)
            except Exception as e:
                self._rx_queue.put(e)
            self.notice()
        else:
            # pragma: no cover
            pass

    def notice(self):
        """
        通知主线程处理
        """
        self._rx_event.set()

    def __del__(self):
        """
        回收引用
        """
        self._tx_event = None
        self._rx_event = None
        self._tx_queue = None
        self._rx_queue = None


if __name__ == '__main__':
    # pragma: no cover
    pass
    # tx = Queue()
    # rx = Queue()
    # tx_event = Event()
    # rx_event = Event()
    # thread = SqliteThread(tx, rx, tx_event, rx_event)
    # thread.start()
    # def test():
    #     return 5555
    # tx.put(test)
    # tx_event.set()
    # rx_event.wait()
    # rx_event.clear()
    # res = rx.get(timeout=0.1)
    # print(res)
    # tx.put('close')
    # tx_event.set()
    # rx_event.wait()
