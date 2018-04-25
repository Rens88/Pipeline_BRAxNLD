import pandas as pd


df = pd.DataFrame()
print(df)

df1 = pd.DataFrame([1, 2],columns = ['A'])
print(df1)

df = df.append(df1)

df2 = pd.DataFrame([3, 4],columns = ['B'])
# df = df.append(df2)
df = pd.concat([df,df2],axis= 1)

df = pd.concat([df,df],axis= 0, ignore_index = True)


print('---')
print(df)