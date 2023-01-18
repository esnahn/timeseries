import requests, zipfile, io
import shutil
from pathlib import Path

x13as_parent = Path(".")
x13as_ascii_path = x13as_parent / "x13as/x13as_ascii.exe"
x13as_old_path = x13as_parent / "x13as/x13as.exe"

# ascii output version
x13as_url = "https://www2.census.gov/software/x-13arima-seats/x13as/windows/program-archives/x13as_ascii-v1-1-b59.zip"

# workaround for statsmodel 0.13.x hardcoding x13 filename
if not x13as_old_path.exists():
    if not x13as_ascii_path.exists():
        x13as_ascii_path.parent.mkdir(exist_ok=True)

        r = requests.get(x13as_url, stream=True)
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            z.extractall(path=x13as_parent)

    # copy new file in old name
    shutil.copy(x13as_ascii_path, x13as_old_path)
else:
    print("alreay exists")
