import os
import re
import queue
import logging
import threading

logger = logging.getLogger(__name__)

KB = 1024
MB = KB**2
GB = KB**3
TB = KB**4
# S3 multi-part upload parts must be larger than 5mb (expect last part)
MIN_S3_SIZE = 5 * MB


def _create_s3_client(session):
    return session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL'))


def _threads(num_threads, data, callback, *args, **kwargs):
    q = queue.Queue()
    item_list = []

    def _thread_run():
        while True:
            item = q.get()
            for _ in range(3):
                # re try 3 times before giving up
                try:
                    response = callback(item, *args, **kwargs)
                except Exception:
                    logger.exception("Retry failed batch of: {}".format(item))
                else:
                    item_list.append(response)
                    break

            q.task_done()

    for i in range(num_threads):
        t = threading.Thread(target=_thread_run)
        t.daemon = True
        t.start()

    # Fill the Queue with the data to process
    for item in data:
        q.put(item)

    # Start processing the data
    q.join()

    return item_list


def _convert_to_bytes(value):
    """Convert the input value to bytes

    Arguments:
        value {string} -- Value and size of the input with no spaces

    Returns:
        float -- The value converted to bytes as a float

    Raises:
        ValueError -- if the input value is not a valid type to convert
    """
    if value is None:
        return None
    value = value.strip()
    sizes = {'KB': 1024,
             'MB': 1024**2,
             'GB': 1024**3,
             'TB': 1024**4,
             }
    if value[-2:].upper() in sizes:
        return float(value[:-2].strip()) * sizes[value[-2:].upper()]
    elif re.match(r'^\d+(\.\d+)?$', value):
        return float(value)
    elif re.match(r'^\d+(\.\d+)?\s?B$', value):
        return float(value[:-1])
    else:
        raise ValueError("Value {} is not a valid size".format(value))
