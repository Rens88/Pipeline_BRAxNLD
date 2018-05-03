import numpy as np

test = np.array([0.1234,0.234])

rounded = np.round(test,2)
print(rounded)

# np.savetxt("C:\\Users\\rensm\\Documents\\SURFDRIVE\\Repositories\\NP repository\\test.csv", rounded, delimiter=",",fmt='%.2f')
print(min(x for x in range(1,300) if x % 3 == 0 if x % 5 == 0))

print(min(x for x in range(1,300) if x % 2 != 0 if x % 2 != 1))
print(min(x for x in range(1,300) if x % 3 == 0 if x % 5 == 0))


