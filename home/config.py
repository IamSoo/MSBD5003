import yaml


class Config():
    cfg = ""
    def loadConfig(self,yamlFile):
        print("Loading Config file {}",yamlFile)
        with open(yamlFile,'r') as file:
            self.cfg = yaml.load(file)

    def getConfig(self):
        return self.cfg