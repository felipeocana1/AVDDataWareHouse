from extract.extractFun import extarcts
from load.loadFun import loads
from transform.trasnformFun import transforms

import traceback


try:
    extarcts()
    ID=transforms()
    loads(ID=1)
    
except:
    traceback.print_exc()
finally:
    pass