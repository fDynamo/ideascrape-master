import time


def log_start(script_name: str = None) -> float:
    start = time.time()
    if not script_name:
        print("Started", start)
    else:
        print("Started", script_name, start)
    return start


def log_end(start_time: float) -> float:
    end = time.time()
    elapsed = end - start_time
    print("Ended", end)
    print("Duration", elapsed, "seconds")
    return elapsed
