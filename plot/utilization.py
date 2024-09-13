import plotly.graph_objects as go
import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate average wait/run and wait/proc from a job file.", allow_abbrev=False)
    parser.add_argument("filename1", help="The name of the file to process.")
    parser.add_argument("filename2", help="The name of the file to process.")
    parser.add_argument("filename3", help="The name of the file to process.")
    args, _ = parser.parse_known_args()
    
    files = [args.filename1, args.filename2, args.filename3]
    colors = ['green', 'orange', 'pink']
    names = ['theta', 'cluster 0', 'cluster 1']

    # Create the Plotly figure
    fig = go.Figure()
    for file, color, name in zip(files, colors, names):

        # Read the file into a DataFrame
        df = pd.read_csv(file, sep=';', header=None, names=['timestamp', 'type', 'value', 'metrics'])
        print(df)

        # Extract the 'uti' value using regex
        df['uti'] = df['metrics'].str.extract(r'uti=([^ ]+)')

        # Convert 'timestamp' to numeric
        df['timestamp'] = pd.to_numeric(df['timestamp'])

        # Convert 'uti' to numeric
        df['uti'] = pd.to_numeric(df['uti'])



        # Add a scatter trace for 'uti' vs 'timestamp'
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['uti'], mode='lines+markers', name=name,  line=dict(color=color)))

    # Update layout
    fig.update_layout(
        title='uti vs Timestamp',
        xaxis_title='Timestamp',
        yaxis_title='uti Value'
    )

    # Show the plot
    fig.show()