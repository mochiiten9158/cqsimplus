import plotly.express as px
import pandas as pd

# Job Data (replace with your actual data)
data = {
    'cpu_count': [1, 2, 4, 1, 2, 3, 4, 2, 1, 3],
    'walltime': [10, 20, 30, 15, 25, 35, 40, 22, 18, 32]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Calculate bin edges for uniform intervals
cpu_bins = pd.interval_range(start=df['cpu_count'].min(), end=df['cpu_count'].max() + 1, freq=1)
walltime_bins = pd.interval_range(start=df['walltime'].min(), end=df['walltime'].max() + 1, freq=10)

# Create the heatmap
fig = px.histogram2d(
    df, 
    x="walltime", 
    y="cpu_count", 
    histfunc="count",  # Count the number of jobs in each bin
    nbinsx=len(walltime_bins) - 1,
    nbinsy=len(cpu_bins) - 1,
    labels={"x": "Walltime", "y": "CPU Count", "color": "Number of Jobs"},
    color_continuous_scale="Viridis",
    title="Job Distribution Heatmap"
)

# Customize layout (optional)
fig.update_layout(xaxis_title="Walltime", yaxis_title="CPU Count")

# Show the plot
fig.show()