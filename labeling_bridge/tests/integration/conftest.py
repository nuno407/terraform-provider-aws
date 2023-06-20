import os
import tempfile

os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"
