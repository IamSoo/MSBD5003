import unittest
from home.jsonToCSVConverter import JsonToCSV
import home.config as config
import os.path

class TestAll(unittest.TestCase):
    def testJsonToCSV(self):
        csvFile = JsonToCSV.getCSVFilePathFromTxtFile("https://s3-ap-southeast-1.amazonaws.com/5003-project/data/airlines.txt")
        self.assertEqual(csvFile,
                         "https://s3-ap-southeast-1.amazonaws.com/5003-project/data/airlines.csv",
                         "Should be end with CSV file")


    def testConfig(self):
        my_path = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(my_path, "test_config.yml")
        print("File path is :" + path)
        confObject = config.Config()
        confObject.loadConfig(path)
        cfg = confObject.getConfig()
        self.assertEqual("MSBD5003",
                         cfg["project"]["name"])

if __name__=='__main__':
    unittest.main()
