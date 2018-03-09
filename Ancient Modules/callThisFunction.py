# 14-11-2017, Rens Meerhoff
# This is an example function that is called by 'callThatFunction.py'

if __name__ == '__main__':
    # test1.py executed as script
    # do something
    some_func(x,y)
    some_other_func(x,y)


def some_func(x,y):
	print('In function: ' + x)
	print('In function: ' + y)
	return[x,y]

def some_other_func(x,y):
	print('In other function: ' + x)
	print('In other function: ' + y)
	return[x,y]

