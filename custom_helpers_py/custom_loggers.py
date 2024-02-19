from datetime import datetime


def log_start(script_name: str) -> datetime:
    script_started_at = datetime.now()
    print("[START]", script_name, script_started_at)

    return script_started_at


def log_end(script_name: str, script_started_at: datetime):
    script_ended_at = datetime.now()
    time_elapsed = (script_ended_at - script_started_at).total_seconds()
    print("[END]", script_name, script_ended_at)
    print("Time elapsed, seconds", time_elapsed)
