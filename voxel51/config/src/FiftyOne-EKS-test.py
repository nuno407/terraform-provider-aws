# -*- coding: utf-8 -*-

import fiftyone as fo
import fiftyone.zoo as foz


dataset_zoo = foz.load_zoo_dataset("quickstart-video")
session = fo.launch_app(dataset_zoo)


session.wait(-1)


