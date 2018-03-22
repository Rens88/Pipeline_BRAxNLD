import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm


a = np.array([[ 1, 2, 3, 4, 5, 6, 7, 8 ],
              [ 9, 8, 7, 6, 5, 4, 3, 2 ]])

categories = np.array([0, 0, 0, 0, 1, 2, 0, 0])

colormap = np.array(['r', 'g', 'b'])
print(colormap[categories])
plt.scatter(a[0], a[1], s=50, c=colormap[categories])
plt.show()
