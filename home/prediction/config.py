import yaml
import os

cfg = "";

currentFodler = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(currentFodler, "sparkConfig.yml"),'r') as file:
    cfg = yaml.load(file,Loader=yaml.FullLoader)
