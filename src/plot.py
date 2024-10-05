"""
Plotter Case study 2

Simulate 2 clusters using CQSim+.
    - Cluster 1 uses original runtime.
    - Cluster 2 runs by a factor of x.

Two cases:
    - The user selects the faster cluster with y% probability
    - Dynamically allocate based on shortest wait time

Scenarios:
    - 



Plots produced:
    - Avg wait time vs Node Count
    - Avg wait time vs Runtime (hrs)

Plots to produce:
    - Avg wait time vs Node count
    - Avg wait time vs Runtime (hrs)

"""
import pandas as pd
from dash import Dash, html, dash_table, dcc
import plotly.express as px
from plotly.express import data
import plotly.graph_objects as go
import numpy as np
from scipy.stats import t

# TODO: Add error bars https://plotly.com/python/error-bars/

def std_dev(x):
    return x.std()

def kurtosis(x):
    return x.kurtosis()


def std_err(x):
    return x.std() / np.sqrt(x.count())


def skewness(x):
    return x.skew()


def ci_up(x):
    # return t.interval(
    #     confidence=0.95,
    #     df=len(x) - 1,
    #     loc=x.mean(),
    #     scale=x.sem())[1] - x.mean()
    return x.quantile(0.75) - x.quantile(0.5)
    return 50000

def ci_down(x):
    # return x.mean() - t.interval(
    #     confidence=0.95,
    #     df=len(x) - 1,
    #     loc=x.mean(),
    #     scale=x.sem())[0]
    return x.quantile(0.5) - x.quantile(0.25)

def create_wait_vs_submit_probvopt(speed):
    '''
    Wait time vs Node Count

    Creates the plot for comparing probable user vs greedy turnaround.
    
    '''
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']

    # Paths to the experiment results
    paths = [
        f'../data/Results/optimal_turnaround_{speed}',
        f'../data/Results/probable_user_{speed}_0.5',
        f'../data/Results/probable_user_{speed}_0.6',
        f'../data/Results/probable_user_{speed}_0.7',
        # f'../data/Results/probable_user_{speed}_0.8',
    ]

    # Names for each plot
    names = [
        f'Greedy Turn Around',
        f'50%',
        f'60%',
        f'70%',
        # f'80%',
    ]

    # Read all the data from each file
    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))

    fig = go.Figure()
    for df, name in zip(dfs, names):
        fig.add_trace(go.Scatter(x=df['submit'], y=df['wait'], name=name, mode='markers'))
    return fig

def create_wait_vs_node_probvopt(speed):
    '''
    Wait time vs Node Count

    Creates the plot for comparing probable user vs greedy turnaround.
    
    '''
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']

    # Paths to the experiment results
    paths = [
        f'../data/Results/optimal_turnaround_{speed}',
        f'../data/Results/probable_user_{speed}_0.5',
        f'../data/Results/probable_user_{speed}_0.6',
        f'../data/Results/probable_user_{speed}_0.7',
        # f'../data/Results/probable_user_{speed}_0.8',
    ]

    # Names for each plot
    names = [
        f'Greedy Turn Around',
        f'50%',
        f'60%',
        f'70%',
        # f'80%',
    ]

    # Read all the data from each file
    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))




    fig = go.Figure()
    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for df, name in zip(dfs, names):
        df['proc_binned'] =pd.cut(df['proc1'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('proc_binned', observed = False)['wait'].mean().reset_index()
        # std_wait_df = df.groupby('proc_binned', observed = False)['wait'].std().reset_index()
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['proc_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        cdown = df.groupby('proc_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('proc_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        fig.add_trace(go.Bar(
            x=avg_wait_df['proc_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
    fig.update_layout(barmode='group')
    return fig


def create_wait_vs_walltime_probvopt(speed):
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    paths = [
        f'../data/Results/optimal_turnaround_{speed}',
        f'../data/Results/probable_user_{speed}_0.5',
        f'../data/Results/probable_user_{speed}_0.6',
        f'../data/Results/probable_user_{speed}_0.7',
        # f'../data/Results/probable_user_{speed}_0.8',
    ]

    names = [
        f'Greedy Turn Around',
        f'50%',
        f'60%',
        f'70%',
        # f'80%'
    ]

    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df['walltime'] = df['walltime']/60 # convert to minutes
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))

    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for df, name in zip(dfs, names):
        df['walltime_binned'] =pd.cut(df['walltime'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('walltime_binned', observed = False)['wait'].mean().reset_index()
        #std_wait_df = df.groupby('walltime_binned', observed = False)['wait'].std().reset_index()
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['walltime_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        cdown = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        fig.add_trace(go.Bar(
            x=avg_wait_df['walltime_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
    fig.update_layout(barmode='group')
    return fig

def create_wait_vs_node_homo(speed):
    '''
    Wait time vs Node Count

    Creates the plot for comparing probable user vs greedy turnaround.
    
    '''
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']

    # Paths to the experiment results
    paths = [
        f'../data/Results/optimal_turnaround_{speed}',
        f'../data/Results/probable_user_{speed}_0.5',
    ]

    # Names for each plot
    names = [
        f'Greedy Turn Around',
        f'Random Choice',
    ]

    # Read all the data from each file
    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))




    fig = go.Figure()
    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for df, name in zip(dfs, names):
        df['proc_binned'] =pd.cut(df['proc1'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('proc_binned', observed = False)['wait'].mean().reset_index()
        # std_wait_df = df.groupby('proc_binned', observed = False)['wait'].std().reset_index()
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['proc_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        cdown = df.groupby('proc_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('proc_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        fig.add_trace(go.Bar(
            x=avg_wait_df['proc_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
    fig.update_layout(barmode='group')
    return fig


def create_wait_vs_walltime_homo(speed):
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    paths = [
        f'../data/Results/optimal_turnaround_{speed}',
        f'../data/Results/probable_user_{speed}_0.5',
    ]

    # Names for each plot
    names = [
        f'Greedy Turn Around',
        f'Random Choice',
    ]

    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df['walltime'] = df['walltime']/60 # convert to minutes
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))

    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for df, name in zip(dfs, names):
        df['walltime_binned'] =pd.cut(df['walltime'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('walltime_binned', observed = False)['wait'].mean().reset_index()
        #std_wait_df = df.groupby('walltime_binned', observed = False)['wait'].std().reset_index()
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['walltime_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        cdown = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        fig.add_trace(go.Bar(
            x=avg_wait_df['walltime_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
    fig.update_layout(barmode='group')
    return fig

def create_wait_vs_node_opt(speeds):
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    paths = []
    names = []
    for speed in speeds:
        paths.append(f'../data/Results/optimal_turnaround_{speed}')
        names.append(f'{speed}')


    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))



    fig = go.Figure()
    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for df, name in zip(dfs, names):
        df['proc_binned'] =pd.cut(df['proc1'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('proc_binned', observed = False)['wait'].mean().reset_index()
        # std_wait_df = df.groupby('proc_binned', observed = False)['wait'].std().reset_index()
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['proc_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        cdown = df.groupby('proc_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('proc_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        fig.add_trace(go.Bar(
            x=avg_wait_df['proc_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
    fig.update_layout(barmode='group')
    return fig


def create_wait_vs_walltime_opt(speeds):
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    paths = []
    names = []
    for speed in speeds:
        paths.append(f'../data/Results/optimal_turnaround_{speed}')
        names.append(f'{speed}')


    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df['walltime'] = df['walltime']/60 # convert to minutes
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))

    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for df, name in zip(dfs, names):
        df['walltime_binned'] =pd.cut(df['walltime'], bins=bins, labels=labels, right=True)
        avg_wait_df = df.groupby('walltime_binned', observed = False)['wait'].mean().reset_index()
        #stddev_wait_df = df.groupby('walltime_binned', observed = False)['wait'].std().reset_index()
        #stderr_wait_df = df.groupby('walltime_binned', observed = False)['wait'].agg([std_err]).reset_index()
        #skewness_wait_df = df.groupby('walltime_binned', observed = False)['wait'].kurtosis().reset_index()
        cdown = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_down]).reset_index()
        cup = df.groupby('walltime_binned', observed = False)['wait'].agg([ci_up]).reset_index()
        cup['ci_up'] = cup['ci_up'].fillna(0)
        cdown['ci_down'] = cdown['ci_down'].fillna(0)
        print(cup['ci_up'])
        print(cdown['ci_down'])
        fig.add_trace(go.Bar(
            x=avg_wait_df['walltime_binned'],
            y=avg_wait_df['wait'],
            error_y= go.bar.ErrorY(
                symmetric=False,
                array=cup['ci_up'],
                arrayminus=cdown['ci_down']

            ),
            name = name
        ))
        # fig.add_trace(go.Box(y=df['wait'], x=df['walltime_binned'], name=name, boxmean=True, boxpoints=False))
    fig.update_layout(barmode='group')
    # fig.update_layout(yaxis_title='Average Wait Time', boxmode='group')
    return fig

def create_wait_vs_walltime_opt_box(speeds):
    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    paths = []
    names = []
    for speed in speeds:
        paths.append(f'../data/Results/optimal_turnaround_{speed}')
        names.append(f'{speed}')


    dfs = []
    for path in paths:
        files = ['theta_2022_0.rst', 'theta_2022_1.rst']
        cluster_dfs = []
        for file in files:
            df = pd.read_csv(f'{path}/Results/{file}', sep=';', header=None) 
            df.columns = column_names
            df['walltime'] = df['walltime']/60 # convert to minutes
            df_sorted = df.sort_values(by='id')
            df_remove_warmup_cooldown = df_sorted.iloc[1000:-1000]
            cluster_dfs.append(df_remove_warmup_cooldown)
        dfs.append(pd.concat(cluster_dfs, ignore_index=True))

    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for df, name in zip(dfs, names):
        df['walltime_binned'] =pd.cut(df['walltime'], bins=bins, labels=labels, right=True)
        # fig.add_trace(go.Bar(
        #     x=avg_wait_df['walltime_binned'],
        #     y=avg_wait_df['wait'],
        #     error_y=dict(type='data', array=std_wait_df['wait'],),
        #     name = name
        # ))
        fig.add_trace(go.Box(y=df['wait'], x=df['walltime_binned'], name=name, boxmean=True, boxpoints=False))
        #fig.add_trace(go.Box(y=df['wait'], x=df['walltime_binned'], name=name, boxmean=True, marker={'opacity': 0}))
        #fig.add_trace(go.Box(y=df['wait'], x=df['walltime_binned'], name=name, boxmean=True, boxpoints=False, lowerfence=df['wait'].quantile(0.25), upperfence=df['wait'].quantile(0.75)))
    #fig.update_layout(barmode='group')
    fig.update_layout(yaxis_title='Average Wait Time', boxmode='group')
    return fig



stddev = False

# Initialize the app
app = Dash()
app.layout = [
    dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, 1.0x)
'''),
    dcc.Graph(figure=create_wait_vs_node_homo(1.0)),
    dcc.Graph(figure=create_wait_vs_walltime_homo(1.0)),
    dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, 1.1x)
'''),
    dcc.Graph(figure=create_wait_vs_node_probvopt(1.1)),
    dcc.Graph(figure=create_wait_vs_walltime_probvopt(1.1)),
        dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, 1.2x)
'''),
    dcc.Graph(figure=create_wait_vs_node_probvopt(1.2)),
    dcc.Graph(figure=create_wait_vs_walltime_probvopt(1.2)),
        dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, 1.25x)
'''),
    dcc.Graph(figure=create_wait_vs_node_probvopt(1.25)),
    dcc.Graph(figure=create_wait_vs_walltime_probvopt(1.25)),
        dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, 1.3x)
'''),
    dcc.Graph(figure=create_wait_vs_node_probvopt(1.3)),
    dcc.Graph(figure=create_wait_vs_walltime_probvopt(1.3)),
    dcc.Graph(figure=create_wait_vs_submit_probvopt(1.3)),
    dcc.Markdown('''
## Cluster 1 (2180, 1.0x) and Cluster 2 (2180, Varied)
'''),
    dcc.Graph(figure=create_wait_vs_node_opt([1.1, 1.2, 1.25, 1.3])),
    dcc.Graph(figure=create_wait_vs_walltime_opt([1.1, 1.2, 1.25, 1.3])),
    dcc.Graph(figure=create_wait_vs_walltime_opt_box([1.1, 1.2, 1.25, 1.3])),
]


# Run the app
if __name__ == '__main__':
    app.run(debug=True)

