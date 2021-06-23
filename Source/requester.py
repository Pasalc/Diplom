import requests
from requests.exceptions import HTTPError
import queue
import threading

class Worker(threading.Thread):
    def __init__(self, q, other_arg, *args, **kwargs):
        self.q = q
        self.other_arg = other_arg
        super().__init__(*args, **kwargs)
    def run(self):
        while True:
            try:
                work = self.q.get(timeout=3)  # 3s timeout
                print(work['i'])
            except queue.Empty:
                return
            # do whatever work you have to do on work
            self.q.task_done()

if __name__ == '__main__':
    """
        print("after app")
        params={'q': 'requests+language:python'}
        headers={'Accept': 'application/vnd.github.v3.text-match+json'}
        data={'file':'e2.jpg'}
        for url in ['http://localhost:5000/upload']:
            try:
                with open('../e3.ini', 'rb') as f:
                    response = requests.post(url,files={'file': ('random.txt', f)})
                # If the response was successful, no Exception will be raised
                response.raise_for_status()
                print(response.content)
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6
            else:
                print('Success!')
    """
    response=requests.get('http://127.0.0.1:5000/connect')
    response.raise_for_status()
    response=requests.get('http://127.0.0.1:5001/connect')
    response.raise_for_status()
    #print("0 {}".format(response.text))
    #response=requests.get('http://127.0.0.1:5001/connect')
    #response.raise_for_status()
    print("1 {}".format(response.text))
    response=requests.post('http://127.0.0.1:5000/start_job?fname=12')
    response.raise_for_status()
    print("2 {}".format(response.text))
    response=requests.get('http://127.0.0.1:5000/disconnect')
    response.raise_for_status()
    response=requests.post('http://127.0.0.1:5001/start_job')
    response.raise_for_status()
    """
    tt=__import__('jobs.my_mod').sample_function
    tt()
    
    import importlib
    
    spec = importlib.util.spec_from_file_location('my_mod', 'jobs')
    module = importlib.util.module_from_spec(spec)
    module.sample_function()
    import asyncio
    import time
    
    async def hel_world(val):
        await asyncio.sleep(val)
        print(val)
    from asyncio.tasks import all_tasks
    def foreverloop(loop):
        print("ever")
        try:
            loop.run_forever()
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
        finally:
            
            pending = all_tasks(loop)
            loop.run_until_complete(asyncio.gather(*pending))
            loop.close()
    loop = asyncio.get_event_loop()
    ttts=[]
    ttts.append(loop.create_task(hel_world(2)))
    ttts.append(loop.create_task(hel_world(1)))
    threading.Thread(target=foreverloop,args=[loop]).start()
    loop.create_task(hel_world(4))
    loop.create_task(hel_world(3))
    loop.run_until_complete(asyncio.gather(*ttts))
    async def say_after(delay, what):
        await asyncio.sleep(delay)
        print(what)

    async def main():
        print(f"started at {time.strftime('%X')}")

        task1 = asyncio.create_task(
        say_after(1, 'hello'))

        task2 = asyncio.create_task(
        say_after(2, 'world'))

        print(f"finished at {time.strftime('%X')}")
        await asyncio.sleep(4)
        print(f"started at {time.strftime('%X')}")
        await task1
        await task2

        print(f"finished at {time.strftime('%X')}")

    asyncio.run(main())
    raise queue.Full
    b=[{'i':i,'val':0} for i in range(10000)]
    otherarg=None
    q = queue.Queue()
    for ptf in b:
        q.put_nowait(ptf)
    for _ in range(20):
        Worker(q, otherarg).start()
    while True:
        if q.empty():  # blocks until the queue is empty.
            print('finaly')
            break
        print('loop')
        
        """