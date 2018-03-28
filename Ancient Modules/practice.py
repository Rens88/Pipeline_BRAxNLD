import numpy as np
import pandas as pd

df = pd.DataFrame(np.arange(12).reshape(3,4),
                      columns=['A', 'B', 'C', 'D'])
df.drop(columns=['B', 'C'])