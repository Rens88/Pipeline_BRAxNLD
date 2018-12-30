import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import InterpolatedUnivariateSpline,interp1d

# given values
xi = np.array([0.2, 0.5, 0.7, 0.9])
yi = np.array([0.3, -0.1, 0.2, 0.1])
# positions to inter/extrapolate
x = np.linspace(0, 1, 50)
# spline order: 1 linear, 2 quadratic, 3 cubic ... 
order = 1
# do inter/extrapolation
s = InterpolatedUnivariateSpline(xi, yi, k=order)
y = s(x)

# example showing the interpolation for linear, quadratic and cubic interpolation
plt.figure()
plt.plot(xi, yi,'o')

for order in range(1, 4):
    s = InterpolatedUnivariateSpline(xi, yi, k=order)
    y = s(x)
    plt.plot(x, y)
plt.show()



# AS FUNCTION
def resampleData(x,y,xnew,order): # order 1) = linear, 2) = quadratic, 3) = cubic
	f = InterpolatedUnivariateSpline(x, y, k=order)
	
	ynew = f(xnew)   # use interpolation function returned by `interp1d`
	# limit ynew to values around original x
	# next(x[0] for x in enumerate(L) if x[1] > 0.7)
	return[ynew]