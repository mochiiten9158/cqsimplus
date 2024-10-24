'''
'''
import pandas as pd
import plotly.graph_objects as go
from plot_utils import read_rst, read_ult
from dash import Dash, dcc, html
from dash import Dash, html, dcc, Input, Output, callback
from plotly.subplots import make_subplots


###################################################################
# Utility functions
###################################################################
def cal_boslow(df):
    '''
    Calcaulte bounded slowdown.
    '''
    return df.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)

def cal_boslow95(df):
    '''
    Calcuate bounded slowdown and only reaturn values in the 95% CI.
    '''
    # Calculate bounded slowdown for each row
    df['bounded_slowdown'] = df.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)

    # Calculate mean and standard deviation of bounded slowdown
    mean_slowdown = df['bounded_slowdown'].mean()
    std_slowdown = df['bounded_slowdown'].std()

    # Calculate 95% confidence interval
    ci_lower = mean_slowdown - 1.96 * std_slowdown
    ci_upper = mean_slowdown + 1.96 * std_slowdown

    # Filter rows based on confidence interval
    df_filtered = df[(df['bounded_slowdown'] >= ci_lower) & (df['bounded_slowdown'] <= ci_upper)]

    return df_filtered

###################################################################
# Wait Time vs Node Count (Binned)
###################################################################
def violin_cmp_2_exp_wait_v_node_count(
        exp1c1,
        exp1c2,
        exp2c1,
        exp2c2,
        exp_1_name,
        exp_2_name,
        ):

    # Read result files 
    c1 = read_rst(exp1c1)
    c2 = read_rst(exp1c2)
    c3 = read_rst(exp2c1)
    c4 = read_rst(exp2c2)

    exp1_net = pd.concat([c1, c2], axis=0)
    exp2_net = pd.concat([c3, c4], axis=0)

    # Remove first 1000 and last 1000 jobs
    # Cluster warmup period
    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['wait_m'] = exp1_net['wait']/60
    exp2_net['wait_m'] = exp2_net['wait']/60

    # Assign each row a bin label for the column 'proc_binned'
    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['proc_binned'] = pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)

    # Plot for each bin
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    showlegend = True
    bin_index = 0  # To keep track of bin index

    total_jobs = len(exp1_net)  # Calculate total jobs for percentage calculation

    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['proc_binned'][exp1_net['proc_binned'] == label],
            y=exp1_net['wait_m'][exp1_net['proc_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='green',
            showlegend=showlegend,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['proc_binned'][exp2_net['proc_binned'] == label],
            y=exp2_net['wait_m'][exp2_net['proc_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=showlegend,
            spanmode = 'hard'
        ))
        showlegend=False

        # Calculate and add mean lines with adjusted x-positions
        mean_exp1 = exp1_net['wait_m'][exp1_net['proc_binned'] == label].mean()
        mean_exp2 = exp2_net['wait_m'][exp2_net['proc_binned'] == label].mean()

        fig.add_shape(type='line', x0=bin_index - 0.5, x1=bin_index, y0=mean_exp1, y1=mean_exp1, 
                              line=dict(color='green', width=2, dash='dash')) 
        fig.add_shape(type='line',x0=bin_index, x1=bin_index + 0.5, y0=mean_exp2, y1=mean_exp2, 
                              line=dict(color='orange', width=2, dash='dash'))

        # Calculate y-range for the current bin
        y_range = [
            min(exp1_net['wait_m'][exp1_net['proc_binned'] == label].min(),
                exp2_net['wait_m'][exp2_net['proc_binned'] == label].min()),
            max(exp1_net['wait_m'][exp1_net['proc_binned'] == label].max(),
                exp2_net['wait_m'][exp2_net['proc_binned'] == label].max())
        ]
        y_offset = (y_range[1] - y_range[0]) * 0.05  # 5% of the y-range

        fig.add_annotation(x=bin_index - 0.25, y=mean_exp1 + y_offset, 
                           text=f"Avg={mean_exp1:.2f}", showarrow=False, 
                           font=dict(color='red'),
                           textangle=-45)  # Add textangle here
        fig.add_annotation(x=bin_index + 0.25, y=mean_exp2 + y_offset,
                           text=f"Avg={mean_exp2:.2f}", showarrow=False,
                           font=dict(color='red'),
                           textangle=-45)   # Add textangle here

        # Calculate job count and percentage for the bin
        bin_count = len(exp1_net['proc_binned'][exp1_net['proc_binned'] == label])
        bin_percent = (bin_count / total_jobs) * 100

        # Add annotation for job count and percentage
        fig.add_annotation(x=bin_index, y=y_range[1] * 1.15,  # Adjust position as needed
                           text=f"Jobs: {bin_count} ({bin_percent:.1f}%)",
                           showarrow=False, font=dict(size=10))

        bin_index += 1

    # Plot the overall value
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    # For the overall case
    exp1_cpy['proc_binned'] = 'Overall'
    exp2_cpy['proc_binned'] = 'Overall'

    fig.add_trace(go.Violin(
        x=exp1_cpy['proc_binned'][exp1_cpy['proc_binned'] == 'Overall'],
        y=exp1_cpy['wait_m'][exp1_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='green',
        showlegend=showlegend,
        spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['proc_binned'],
        y=exp2_cpy['wait_m'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=showlegend,
        spanmode = 'hard'
    ))

    mean_exp1_overall = exp1_cpy['wait_m'].mean()
    mean_exp2_overall = exp2_cpy['wait_m'].mean()

    fig.add_shape(go.Line(x0=bin_index - 0.5, x1=bin_index, y0=mean_exp1_overall, y1=mean_exp1_overall, 
                          line=dict(color='green', width=2, dash='dash')))  
    fig.add_shape(go.Line(x0=bin_index, x1=bin_index + 0.5, y0=mean_exp2_overall, y1=mean_exp2_overall, 
                          line=dict(color='orange', width=2, dash='dash'))) 
    
    y_range_overall = [
        min(exp1_cpy['wait_m'].min(),
            exp2_cpy['wait_m'].min()),
        max(exp1_cpy['wait_m'].max(),
            exp2_cpy['wait_m'].max())
    ]
    y_offset_overall = (y_range_overall[1] - y_range_overall[0]) * 0.05  # 5% of the y-range

    # Add annotations for 'Overall' mean values with dynamic vertical offset
    fig.add_annotation(x=bin_index - 0.25, y=mean_exp1_overall + y_offset_overall,
                       text=f"Avg={mean_exp1_overall:.2f}", showarrow=False, 
                       font=dict(color='red'),
                       textangle=-45)  # Add textangle here
    fig.add_annotation(x=bin_index + 0.25, y=mean_exp2_overall + y_offset_overall, 
                       text=f"Avg={mean_exp2_overall:.2f}", showarrow=False,
                       font=dict(color='red'),
                       textangle=-45)

    # Add annotation for overall job count and percentage (which is 100%)
    fig.add_annotation(x=bin_index, y=y_range_overall[1] * 1.1,  # Adjust position as needed
                       text=f"Jobs: {total_jobs} (100%)",
                       showarrow=False, font=dict(size=10))

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
            y=1.15,
            xanchor="left",
            x=0.01,
            orientation='h'
        ),
        yaxis2=dict(title="Job Count")
    )

    return [
        dcc.Markdown(
                        f"""
Wait time (min) vs Job Size
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]


###################################################################
# Wait Time vs Walltime (Binned)
###################################################################
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

    bins = [10, 30, 60, 120, 250, 500, 1000, 1500]
    labels = ['(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['walltime_binned'] = pd.cut(_df['walltime_m'], bins=bins, labels=labels, right=True)
    
    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['walltime_binned'] = 'Overall'
    exp2_cpy['walltime_binned'] = 'Overall'

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    bin_index = 0
    
    total_jobs = len(exp1_net)  # Calculate total jobs for percentage calculation

    for label in labels:
        fig.add_trace(go.Violin(
            x=exp1_net['walltime_binned'][exp1_net['walltime_binned'] == label],
            y=exp1_net['wait_m'][exp1_net['walltime_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='green',
            showlegend=False,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['walltime_binned'][exp2_net['walltime_binned'] == label],
            y=exp2_net['wait_m'][exp2_net['walltime_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=False,
            spanmode = 'hard'
        ))
        # Calculate and add mean lines with adjusted x-positions
        mean_exp1 = exp1_net['wait_m'][exp1_net['walltime_binned'] == label].mean()
        mean_exp2 = exp2_net['wait_m'][exp2_net['walltime_binned'] == label].mean()

        fig.add_shape(type='line', x0=bin_index - 0.5, x1=bin_index, y0=mean_exp1, y1=mean_exp1, 
                              line=dict(color='green', width=2, dash='dash')) 
        fig.add_shape(type='line',x0=bin_index, x1=bin_index + 0.5, y0=mean_exp2, y1=mean_exp2, 
                              line=dict(color='orange', width=2, dash='dash'))

        # Calculate y-range for the current bin
        y_range = [
            min(exp1_net['wait_m'][exp1_net['walltime_binned'] == label].min(),
                exp2_net['wait_m'][exp2_net['walltime_binned'] == label].min()),
            max(exp1_net['wait_m'][exp1_net['walltime_binned'] == label].max(),
                exp2_net['wait_m'][exp2_net['walltime_binned'] == label].max())
        ]
        y_offset = (y_range[1] - y_range[0]) * 0.05  # 5% of the y-range

        # Add annotations with mean values and dynamic vertical offset
        fig.add_annotation(x=bin_index - 0.25, y=mean_exp1 + y_offset, 
                           text=f"Avg={mean_exp1:.2f}", showarrow=False, 
                           font=dict(color='red'),
                       textangle=-45)
        fig.add_annotation(x=bin_index + 0.25, y=mean_exp2 + y_offset,
                           text=f"Avg={mean_exp2:.2f}", showarrow=False,
                           font=dict(color='red'),
                       textangle=-45)

        # Calculate job count and percentage for the bin
        bin_count = len(exp1_net['walltime_binned'][exp1_net['walltime_binned'] == label])
        bin_percent = (bin_count / total_jobs) * 100

        # Add annotation for job count and percentage
        fig.add_annotation(x=bin_index, y=y_range[1] * 1.15,  # Adjust position as needed
                           text=f"Jobs: {bin_count} ({bin_percent:.1f}%)",
                           showarrow=False, font=dict(size=10))

        bin_index += 1

    fig.add_trace(go.Violin(
        x=exp1_cpy['walltime_binned'],
        y=exp1_cpy['wait_m'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='green',
        showlegend=True,
        spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['walltime_binned'],
        y=exp2_cpy['wait_m'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=True,
        spanmode = 'hard'
    ))


    mean_exp1_overall = exp1_cpy['wait_m'].mean()
    mean_exp2_overall = exp2_cpy['wait_m'].mean()

    fig.add_shape(go.Line(x0=bin_index - 0.5, x1=bin_index, y0=mean_exp1_overall, y1=mean_exp1_overall, 
                          line=dict(color='green', width=2, dash='dash')))  
    fig.add_shape(go.Line(x0=bin_index, x1=bin_index + 0.5, y0=mean_exp2_overall, y1=mean_exp2_overall, 
                          line=dict(color='orange', width=2, dash='dash'))) 

    # Add annotations for 'Overall' mean values
    y_range_overall = [
        min(exp1_cpy['wait_m'].min(),
            exp2_cpy['wait_m'].min()),
        max(exp1_cpy['wait_m'].max(),
            exp2_cpy['wait_m'].max())
    ]
    y_offset_overall = (y_range_overall[1] - y_range_overall[0]) * 0.05  # 5% of the y-range

    # Add annotations for 'Overall' mean values with dynamic vertical offset
    fig.add_annotation(x=bin_index - 0.25, y=mean_exp1_overall + y_offset_overall,
                       text=f"Avg={mean_exp1_overall:.2f}", showarrow=False, 
                       font=dict(color='red'),
                       textangle=-45)
    fig.add_annotation(x=bin_index + 0.25, y=mean_exp2_overall + y_offset_overall, 
                       text=f"Avg={mean_exp2_overall:.2f}", showarrow=False,
                       font=dict(color='red'),
                       textangle=-45)

    # Add annotation for overall job count and percentage (which is 100%)
    fig.add_annotation(x=bin_index, y=y_range_overall[1] * 1.1,  # Adjust position as needed
                       text=f"Jobs: {total_jobs} (100%)",
                       showarrow=False, font=dict(size=10))
    
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
        title="Violin Plot of Wait Time Distribution by Job Walltime and Experiment",
        xaxis_title="Job Walltime (min)",
        yaxis_title="Wait Time (min)",
        legend=dict(
            yanchor="top",
            y=1.15,
            xanchor="left",
            x=0.01,
            orientation='h'
        ),
        xaxis={'categoryarray': labels + ['Overall'], 'categoryorder': 'array'} # Include 'Overall' in x-axis categories
    )

    return [
        dcc.Markdown(
                        f"""
Wait time (min) vs Job Walltime (min)
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]



###################################################################
# Bounded Slowdown vs Node Counts (Binned)
###################################################################
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
            showlegend=showlegend,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['proc_binned'][exp2_net['proc_binned'] == label],
            y=exp2_net['bounded_slowdown'][exp2_net['proc_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=showlegend,
            spanmode = 'hard'
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
        showlegend=showlegend,
        spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['proc_binned'][exp2_cpy['proc_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=showlegend,
        spanmode = 'hard'
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
        ),
        xaxis={'categoryarray': labels, 'categoryorder': 'array'}
    )


    return [
        dcc.Markdown(
                        f"""
Bounded Slowdown vs Job Size
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]



###################################################################
# Bounded Slowdown vs Walltime (Binned)
###################################################################
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
            showlegend=False,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['walltime_binned'][exp2_net['walltime_binned'] == label],
            y=exp2_net['bounded_slowdown'][exp2_net['walltime_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=False,
            spanmode = 'hard'
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
        showlegend=True,
        spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['walltime_binned'][exp2_cpy['walltime_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=True,
        spanmode = 'hard'
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
Bounded Slowdown vs Job Walltime (min)
"""
                    ),
        dcc.Graph(
                figure = fig
            ),
    ]


###################################################################
# Bounded Slowdown in 95% CI vs Walltime (Binned)
###################################################################
def violin_cmp_2_exp_boslo_v_walltime_95(
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

    # Remove first 1000 and last 1000 jobs
    # Cluster warmup period
    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['walltime'] = exp1_net['walltime'] / 60
    exp2_net['walltime'] = exp2_net['walltime'] / 60

    exp1_net = cal_boslow95(exp1_net)
    exp2_net = cal_boslow95(exp2_net)

    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]',
              '(1500, max]']
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
            y=exp1_net[exp1_net['walltime_binned'] == label]['bounded_slowdown'],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=False,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['walltime_binned'][exp2_net['walltime_binned'] == label],
            y=exp2_net[exp2_net['walltime_binned'] == label]['bounded_slowdown'],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=False,
            spanmode = 'hard'
        ))
    fig.add_trace(go.Violin(
        x=exp1_cpy['walltime_binned'][exp1_cpy['walltime_binned'] == 'Overall'],
        y=exp1_cpy['bounded_slowdown'][exp1_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='blue',
        showlegend=True,
        spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['walltime_binned'][exp2_cpy['walltime_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['walltime_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=True,
        spanmode = 'hard'
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
Bounded Slowdown in 95% CI vs Job Walltime (min)
"""
        ),
        dcc.Graph(
            figure=fig
        ),
    ]


###################################################################
# Bounded Slowdown in 95% CI vs Node Count (Binned)
###################################################################
def violin_cmp_2_exp_boslo_v_node_count_95(
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

    # Remove first 1000 and last 1000 jobs
    # Cluster warmup period
    exp1_net = exp1_net.sort_values(by='id')
    exp2_net = exp2_net.sort_values(by='id')  # Fixed typo here, should be exp2_net
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net = cal_boslow95(exp1_net)
    exp2_net = cal_boslow95(exp2_net)

    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]', '(256, 512]', '(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([exp1_net, exp2_net], [exp_1_name, exp_2_name]):
        _df['proc_binned'] = pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)

    exp1_cpy = exp1_net.copy()
    exp2_cpy = exp2_net.copy()

    exp1_cpy['proc_binned'] = 'Overall'
    exp2_cpy['proc_binned'] = 'Overall'

    showlegend = True
    fig = go.Figure()
    for label in labels:
        # Calculate 95% confidence interval for each bin
        exp1_bin = exp1_net[exp1_net['proc_binned'] == label]['bounded_slowdown']
        exp2_bin = exp2_net[exp2_net['proc_binned'] == label]['bounded_slowdown']

        fig.add_trace(go.Violin(
            x=exp1_net['proc_binned'][exp1_net['proc_binned'] == label],
            y=exp1_net['bounded_slowdown'][exp1_net['proc_binned'] == label],
            legendgroup=exp_1_name,
            scalegroup=label,
            name=exp_1_name,
            side='negative',
            line_color='blue',
            showlegend=showlegend,
            spanmode = 'hard'
        ))
        fig.add_trace(go.Violin(
            x=exp2_net['proc_binned'][exp2_net['proc_binned'] == label],
            y=exp2_net['bounded_slowdown'][exp2_net['proc_binned'] == label],
            legendgroup=exp_2_name,
            scalegroup=label,
            name=exp_2_name,
            side='positive',
            line_color='orange',
            showlegend=showlegend,
            spanmode = 'hard'
        ))
        showlegend = False

    fig.add_trace(go.Violin(
        x=exp1_cpy['proc_binned'][exp1_cpy['proc_binned'] == 'Overall'],
        y=exp1_cpy['bounded_slowdown'][exp1_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_1_name,
        scalegroup='Overall',
        name=exp_1_name,
        side='negative',
        line_color='green',
        showlegend=showlegend,
            spanmode = 'hard'
    ))
    fig.add_trace(go.Violin(
        x=exp2_cpy['proc_binned'][exp2_cpy['proc_binned'] == 'Overall'],
        y=exp2_cpy['bounded_slowdown'][exp2_cpy['proc_binned'] == 'Overall'],
        legendgroup=exp_2_name,
        scalegroup='Overall',
        name=exp_2_name,
        side='positive',
        line_color='orange',
        showlegend=showlegend,
            spanmode = 'hard'
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
Bounded Slowdown in 95% CI vs Job Size
"""
        ),
        dcc.Graph(
            figure=fig
        ),
    ]

###################################################################
# Average utilization vs Time (Per Day)
###################################################################
def line_cmp_2_exp_avg_uti_v_time(
        exp1c1,
        exp2c1,
        exp_1_name,
        exp_2_name,
        start_time_offset = 0,
        line_colors = ['orange', 'green']
    ):

    c1 = read_ult(exp1c1)
    # c2 = read_ult(exp1c2)
    c3 = read_ult(exp2c1)
    # c4 = read_ult(exp2c2)
    
    fig = go.Figure()

    exp1_net = pd.concat([c1], axis=0)
    exp2_net = pd.concat([c3], axis=0)

    # Remove first 1000 and last 1000 jobs
    # Cluster warmup period
    exp1_net = exp1_net.sort_values(by='timestamp')
    exp2_net = exp2_net.sort_values(by='timestamp')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['timestamp'] = exp1_net['timestamp'] + start_time_offset
    exp2_net['timestamp'] = exp2_net['timestamp'] + start_time_offset


    # Convert UNIX timestamps to datetime objects
    exp1_net['day'] = pd.to_datetime(exp1_net['timestamp'], unit='s').dt.date
    exp2_net['day'] = pd.to_datetime(exp2_net['timestamp'], unit='s').dt.date

    # Calculate average utilization for each day
    avg_utilization_exp1 = exp1_net.groupby('day')['utilization'].mean()
    avg_utilization_exp2 = exp2_net.groupby('day')['utilization'].mean()

    fig.add_trace(go.Scatter(
        x=avg_utilization_exp1.index,
        y=avg_utilization_exp1.values,
        name=exp_1_name,
        line_color= line_colors[0]
    ))

    fig.add_trace(go.Scatter(
        x=avg_utilization_exp2.index,
        y=avg_utilization_exp2.values,
        name=exp_2_name,
        line_color= line_colors[1]
    ))

    # Customize the layout
    # fig.update_layout(
    #     title="Average Utilization per Day",
    #     xaxis_title="Day",
    #     yaxis_title="Average Utilization",
    #     barmode='group'  # Grouped bar mode
    # )

    # Customize the layout
    fig.update_layout(
        title="Average Utilization per Day",
        xaxis_title="Day",
        yaxis_title="Average Utilization",
        legend=dict(
            yanchor="top",
            y=1.15,
            xanchor="left",
            x=0.01,
            orientation='h'
        ),
    )

    return [
        dcc.Markdown(
            f"""
Avg Utilization Per Day
"""
        ),
        dcc.Graph(
            figure=fig
        ),
    ]

###################################################################
# Job Submit Events Per Day
###################################################################
def line_job_submit_events_per_day(
        exp1c1,
        exp1c2,
        exp_1_name,
        exp_2_name,
        start_time_offset = 0,
        line_colors = ['orange', 'green']
    ):

    c1 = read_ult(exp1c1)
    c2 = read_ult(exp1c2)
    
    fig = go.Figure()

    exp1_net = pd.concat([c1], axis=0)
    exp2_net = pd.concat([c2], axis=0)

    # Remove first 1000 and last 1000 jobs
    # Cluster warmup period
    exp1_net = exp1_net.sort_values(by='timestamp')
    exp2_net = exp2_net.sort_values(by='timestamp')
    exp1_net = exp1_net.iloc[1000:-1000]
    exp2_net = exp2_net.iloc[1000:-1000]

    exp1_net['timestamp'] = exp1_net['timestamp'] + start_time_offset
    exp2_net['timestamp'] = exp2_net['timestamp'] + start_time_offset


    # Convert UNIX timestamps to datetime objects
    exp1_net['day'] = pd.to_datetime(exp1_net['timestamp'], unit='s').dt.date
    exp2_net['day'] = pd.to_datetime(exp2_net['timestamp'], unit='s').dt.date


    count_submit_events_exp1 = exp1_net[exp1_net['event_type'] == 'S'].groupby('day').size()
    count_submit_events_exp2 = exp2_net[exp2_net['event_type'] == 'S'].groupby('day').size()

    fig.add_trace(go.Scatter(
        x=count_submit_events_exp1.index,
        y=count_submit_events_exp1.values,
        name=exp_1_name,
        line_color=line_colors[0]
    ))

    fig.add_trace(go.Scatter(
        x=count_submit_events_exp2.index,
        y=count_submit_events_exp2.values,
        name=exp_2_name,
        line_color=line_colors[1]
    ))

    # Customize the layout
    fig.update_layout(
        title="Job Submitted per Day",
        xaxis_title="Day",
        yaxis_title="No. of Jobs Submitted",
        legend=dict(
            yanchor="top",
            y=1.15,
            xanchor="left",
            x=0.01,
            orientation='h'
        ),
    )

    return [
        dcc.Markdown(
            f"""
Number of jobs submitted per day
"""
        ),
        dcc.Graph(
            figure=fig
        ),
    ]