import pandas as pd



column_names = ['waittime', 'proc']
data = [
    [100, 9],
    [100, 12],
    [100, 22],
    [100, 7],
    [100, 15],
    [100, 29],
]

df = pd.DataFrame(data)
df.columns = column_names

print(df)


bins = [0, 10, 20, 30, float('inf')]
labels = ['(0, 10]','(10, 20]','(20, 30]','(30, max]']
df['proc_bin'] =pd.cut(df['proc'], bins=bins, labels=labels, right=True)

print(df)

# Calculate the mean for each 'proc_bin'
mean_proc = df.groupby('proc_bin', observed=True)['waittime'].mean()

# Print the result
print(mean_proc)