import json
import pandas as pd
from io import StringIO


def convertJSONToCSV(file):
    df = pd.read_json(file)
    csvFile = getCSVFilePathFromTxtFile(file)
    df.to_csv(csvFile,index=True,encoding='utf-8')

def getCSVFilePathFromTxtFile(file):
    indexOfDot = file.rfind(".")
    return file[:indexOfDot] +".csv"

