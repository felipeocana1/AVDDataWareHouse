from jproperties import Properties

configs = Properties()

with open('.properties', 'rb') as config_file:
    configs.load(config_file) 


def getProperty(nameProperty):
    return configs.get(nameProperty).data