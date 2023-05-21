import glob
from pathlib import Path

folder_path = "./input"


def read_job_files(job_number: str):
    folder = Path(folder_path)
    job_files = glob.glob(str(folder / f"*{job_number}*"))
    return job_files
