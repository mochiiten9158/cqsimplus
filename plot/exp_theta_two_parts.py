'''

Metrics:
- wait time delta is the (actual wait time - experiment wait time). If this is positive then the job had lower wait time, otherwise the job suffered.

Plots:
- Scatter plot of delta wait time (minutes) vs submit time (mm-dd-yyyy)

- Scatter plot of delta bounded slowdown vs submit time (mm-dd-yyyy)

- Bar plot of no of jobs vs proc count bins for points whose wait delta is positive

- Bar plot of no of jobs vs proc count bins for points whose wait delta is negative

- Bar plot of no of jobs vs runtime bins for points whose wait delta is positive

- Bar plot of no of jobs vs runtime bins for points whose wait delta is negative


'''
import pandas as pd
import plotly.graph_objects as go
from plot_utils import read_rst, read_ult
from dash import Dash, dcc, html
from dash import Dash, html, dcc, Input, Output, callback




def violin_cmp_2_exp_wait_v_node_count(
        exp1c1,
        exp1c2,
        exp2c1,
        exp2c2,
        exp_1_name,
        exp_2_name,
        ):
    
    c1 = read_rst(exp1c1)
    c2 = read_rst(exp1c2)
    c3 = read_rst(exp2c1)
    c4 = read_rst(exp2c2)

    fig = go.Figure()

    exp1_net = pd.concat([c1, c2], axis=0)
    exp2_net = pd.concat([c3, c4], axis=0)

    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['wait_m'] = exp1_net['wait']/60
    exp2_net['wait_m'] = exp2_net['wait']/60

    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['proc_binned'] = pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)
    
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['proc_binned'] = 'Overall'
    exp2_cpy['proc_binned'] = 'Overall'

    showlegend = True
    fig = go.Figure()
    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['proc_binned'][exp1_net['proc_binned'] == label],
            y=exp1_net['wait_m'][exp1_net['proc_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=showlegend
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['proc_binned'][exp2_net['proc_binned'] == label],
            y=exp2_net['wait_m'][exp2_net['proc_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=showlegend
        ))
        showlegend=False
        
    fig.add_trace(go.Violin(
        x=exp1_cpy['proc_binned'][exp1_cpy['proc_binned'] == 'Overall'],
        y=exp1_cpy['wait_m'][exp1_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='blue',
        showlegend=showlegend
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['proc_binned'][exp2_cpy['proc_binned'] == 'Overall'],
        y=exp2_cpy['wait_m'][exp2_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=showlegend
    ))

    fig.update_traces(
        meanline_visible=True,
        points=False,
        jitter=0.05,
        scalemode='count'
    )
    fig.update_layout(
        violingap=0, 
        violingroupgap=0,
        violinmode='overlay', 
        title="Violin Plot of Wait Time Distribution by Job Size and Experiment",
        xaxis_title="Job Size",
        yaxis_title="Wait Time (min)",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )

    return [
        dcc.Markdown(
                        f"""
TODO: ADD description
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]


def violin_cmp_2_exp_wait_v_walltime(
        exp1c1,
        exp1c2,
        exp2c1,
        exp2c2,
        exp_1_name,
        exp_2_name,
        ):
    
    c1 = read_rst(exp1c1)
    c2 = read_rst(exp1c2)
    c3 = read_rst(exp2c1)
    c4 = read_rst(exp2c2)

    fig = go.Figure()

    exp1_net = pd.concat([c1, c2], axis=0)
    exp2_net = pd.concat([c3, c4], axis=0)

    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['walltime_m'] = exp1_net['walltime']/60
    exp2_net['walltime_m'] = exp2_net['walltime']/60

    exp1_net['wait_m'] = exp1_net['wait']/60
    exp2_net['wait_m'] = exp2_net['wait']/60

    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['walltime_binned'] = pd.cut(_df['walltime_m'], bins=bins, labels=labels, right=True)
    
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['walltime_binned'] = 'Overall'
    exp2_cpy['walltime_binned'] = 'Overall'

    fig = go.Figure()
    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['walltime_binned'][exp1_net['walltime_binned'] == label],
            y=exp1_net['wait_m'][exp1_net['walltime_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=False
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['walltime_binned'][exp2_net['walltime_binned'] == label],
            y=exp2_net['wait_m'][exp2_net['walltime_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=False
        ))
        
    fig.add_trace(go.Violin(
        x=exp1_cpy['walltime_binned'][exp1_cpy['walltime_binned'] == 'Overall'],
        y=exp1_cpy['wait_m'][exp1_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='blue',
        showlegend=True
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['walltime_binned'][exp2_cpy['walltime_binned'] == 'Overall'],
        y=exp2_cpy['wait_m'][exp2_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=True
    ))

    fig.update_traces(
        meanline_visible=True,
        points=False,
        jitter=0.05,
        scalemode='count'
    )
    fig.update_layout(
        violingap=0, 
        violingroupgap=0,
        violinmode='overlay', 
        title="Violin Plot of Wait Time Distribution by Job Size and Experiment",
        xaxis_title="Job Walltime (min)",
        yaxis_title="Wait Time (min)",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )


    return [
        dcc.Markdown(
                        f"""
TODO: ADD description
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]

def cal_boslow(df):
    return df.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)

def violin_cmp_2_exp_boslo_v_node_count(
        exp1c1,
        exp1c2,
        exp2c1,
        exp2c2,
        exp_1_name,
        exp_2_name,
        ):
    
    c1 = read_rst(exp1c1)
    c2 = read_rst(exp1c2)
    c3 = read_rst(exp2c1)
    c4 = read_rst(exp2c2)

    fig = go.Figure()

    exp1_net = pd.concat([c1, c2], axis=0)
    exp2_net = pd.concat([c3, c4], axis=0)

    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['bounded_slowdown'] = cal_boslow(exp1_net)
    exp2_net['bounded_slowdown'] = cal_boslow(exp2_net)

    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['proc_binned'] = pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)
    
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['proc_binned'] = 'Overall'
    exp2_cpy['proc_binned'] = 'Overall'

    showlegend = True
    fig = go.Figure()
    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['proc_binned'][exp1_net['proc_binned'] == label],
            y=exp1_net['bounded_slowdown'][exp1_net['proc_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=showlegend
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['proc_binned'][exp2_net['proc_binned'] == label],
            y=exp2_net['bounded_slowdown'][exp2_net['proc_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=showlegend
        ))
        showlegend=False
        
    fig.add_trace(go.Violin(
        x=exp1_cpy['proc_binned'][exp1_cpy['proc_binned'] == 'Overall'],
        y=exp1_cpy['bounded_slowdown'][exp1_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='blue',
        showlegend=showlegend
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['proc_binned'][exp2_cpy['proc_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=showlegend
    ))

    fig.update_traces(
        meanline_visible=True,
        points=False,
        jitter=0.05,
        scalemode='count'
    )
    fig.update_layout(
        violingap=0, 
        violingroupgap=0,
        violinmode='overlay', 
        title="Violin Plot of Wait Time Distribution by Job Size and Experiment",
        xaxis_title="Job Size",
        yaxis_title="Bounded Slowdown",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )


    return [
        dcc.Markdown(
                        f"""
TODO: ADD description
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]


def violin_cmp_2_exp_boslo_v_walltime(
        exp1c1,
        exp1c2,
        exp2c1,
        exp2c2,
        exp_1_name,
        exp_2_name,
        ):
    
    c1 = read_rst(exp1c1)
    c2 = read_rst(exp1c2)
    c3 = read_rst(exp2c1)
    c4 = read_rst(exp2c2)

    fig = go.Figure()

    exp1_net = pd.concat([c1, c2], axis=0)
    exp2_net = pd.concat([c3, c4], axis=0)

    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['walltime'] = exp1_net['walltime']/60
    exp2_net['walltime'] = exp2_net['walltime']/60

    exp1_net['bounded_slowdown'] = cal_boslow(exp1_net)
    exp2_net['bounded_slowdown'] = cal_boslow(exp2_net)

    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['walltime_binned'] = pd.cut(_df['walltime'], bins=bins, labels=labels, right=True)
    
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['walltime_binned'] = 'Overall'
    exp2_cpy['walltime_binned'] = 'Overall'

    fig = go.Figure()
    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['walltime_binned'][exp1_net['walltime_binned'] == label],
            y=exp1_net['bounded_slowdown'][exp1_net['walltime_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=False
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['walltime_binned'][exp2_net['walltime_binned'] == label],
            y=exp2_net['bounded_slowdown'][exp2_net['walltime_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=False
        ))
        # showlegend=False
        
    fig.add_trace(go.Violin(
        x=exp1_cpy['walltime_binned'][exp1_cpy['walltime_binned'] == 'Overall'],
        y=exp1_cpy['bounded_slowdown'][exp1_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='blue',
        showlegend=True
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['walltime_binned'][exp2_cpy['walltime_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=True
    ))

    fig.update_traces(
        meanline_visible=True,
        points=False,
        jitter=0.05,
        scalemode='count'
    )
    fig.update_layout(
        violingap=0, 
        violingroupgap=0,
        violinmode='overlay', 
        title="Violin Plot of Bounded Slowdown Distribution by Job Size and Experiment",
        xaxis_title="Job Walltime (min)",
        yaxis_title="Bounded Slowdown",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )


    return [
        dcc.Markdown(
                        f"""
TODO: ADD description
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]
