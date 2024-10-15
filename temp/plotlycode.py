import pandas as pd
import plotly.graph_objects as go
from plot_utils import read_rst, read_ult
from dash import Dash, dcc, html
from dash import Dash, html, dcc, Input, Output, callback

def build_figures(
    cluster_1,
    cluster_2,
    theta,
    start = -1,
    end = -1
        
):
    figs = {}
    start_time = 1641021254

    # Optimal turnaround
    c1 = read_rst(cluster_1)
    c2 = read_rst(cluster_2)
    t = read_rst(theta)
        

    # Figure for wait time delta vs submit time
    fig = go.Figure()


    c1['bounded_slow'] = c1.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)
    c2['bounded_slow'] = c2.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)
    t['bounded_slow'] = t.apply(lambda row: (row['wait'] + row['run']) / row['run'] if row['run'] > 10 else (row['wait'] + row['run']) / 10, axis=1)

    merged_c1 = pd.merge(c1, t[['id', 'wait', 'bounded_slow']], on='id', how='left', suffixes=('', '_t'))
    merged_c2 = pd.merge(c2, t[['id', 'wait', 'bounded_slow']], on='id', how='left', suffixes=('', '_t'))

    merged_c1['wait_delta'] = merged_c1['wait_t'] - merged_c1['wait']
    merged_c2['wait_delta'] = merged_c2['wait_t'] - merged_c2['wait']

    merged_c1['bs_delta'] = merged_c1['bounded_slow_t'] - merged_c1['bounded_slow']
    merged_c2['bs_delta'] = merged_c2['bounded_slow_t'] - merged_c2['bounded_slow']

    c1['wait_delta'] = merged_c1['wait_delta']/60
    c2['wait_delta'] = merged_c2['wait_delta']/60


    c1['bs_delta'] = merged_c1['bs_delta']
    c2['bs_delta'] = merged_c2['bs_delta']

    c1['realtime'] = c1['submit'] + start_time
    c2['realtime'] = c2['submit'] + start_time

    c1['datetime'] = pd.to_datetime(c1['realtime'], unit='s')
    c2['datetime'] = pd.to_datetime(c2['realtime'], unit='s')

    c1_nz =  c1[c1['wait_delta'] != 0]
    c2_nz =  c2[c2['wait_delta'] != 0]

    fig.add_trace(go.Scatter(x=c1_nz['datetime'], y=c1_nz['wait_delta'],
                    mode='markers',
                    name='cluster 1'))
    
    fig.add_trace(go.Scatter(x=c2_nz['datetime'], y=c2_nz['wait_delta'],
                mode='markers',
                name='cluster 2'))
    fig.update_layout(
        title='Wait time (theta) - Wait time (cluster X) vs Submit Time',
        xaxis_title='Submit Time',
        yaxis_title='Delta Wait Time (mins)',
    )

    figs['wait_delta_vs_submit'] = {
        'fig' : fig,
        'md' : """
Do jobs overall improve or degrade?
"""

    }

    # Figure bounded slowdown vs submit time
    fig = go.Figure()

    c1_nz =  c1[c1['bs_delta'] != 0]
    c2_nz =  c2[c2['bs_delta'] != 0]


    fig.add_trace(go.Scatter(x=c1_nz['datetime'], y=c1_nz['bs_delta'],
                    mode='markers',
                    name='cluster 1'))
    
    fig.add_trace(go.Scatter(x=c2_nz['datetime'], y=c2_nz['bs_delta'],
                mode='markers',
                name='cluster 2'))
    
    fig.update_layout(
        title='Bounded Slowdown (theta) - Bounded Slowdown (cluster X) vs Submit Time',
        xaxis_title='Submit Time',
        yaxis_title='Delta Bounded Slowdown',
    )



    figs['bs_delta_vs_submit'] = {
        'fig' : fig,
        'md' : """
Bounded slowdown
"""

    }


    # Figure bar plot job count vs proc count bins for positive delta vs negative delta
    fig = go.Figure()
    
    all = pd.concat([c1, c2], axis=0)
    all['walltime'] = all['walltime']/60

    df_p = all[all['wait_delta'] > 0]
    df_n = all[all['wait_delta'] < 0]
    df_0 = all[all['wait_delta'] == 0]

    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([df_p, df_n, df_0], ['Improve', 'Degrade', 'Unchanged']):
        _df['proc_binned'] =pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)
        category_counts = _df['proc_binned'].value_counts()
        # fig.add_trace(
        #     go.Bar(
        #         x=category_counts.index,
        #         y=category_counts.values,
        #         name=name
        #     )
        # )
        fig.add_trace(go.Histogram(
            x=category_counts.index,
            y=category_counts.values,
            histfunc='sum',
            name=name,
            texttemplate='%{y:.2f}%',
            textfont_size=10,
            xbins=dict(size=1)
        ))
    # fig.update_layout(barmode='stack', xaxis={'categoryorder':'array', 'categoryarray':labels})
    fig.update_layout(
        barmode='relative',
        barnorm='percent',
        title='Node Count vs Job Count (%)',
        xaxis_title='Node Count',
        yaxis_title='Job Count (%)',
        yaxis=dict(
            tickformat='.0%',
            tickvals=[i for i in range(0, 110, 10)],
            ticktext=['{:.1f}%'.format(i) for i in range(0, 110, 10)])
    )



    figs['counts_vs_proc'] = {
        'fig' : fig,
        'md' : """
Which kind of jobs improve, degrade?
"""
    }



    # Figure violin plot for delta wait time vs proc count
    fig = go.Figure()
    showlegend = True
    for bin_label in labels:
        fig.add_trace(go.Violin(
            x=df_p['proc_binned'][df_p['proc_binned'] == bin_label],
            y=df_p['wait_delta'][df_p['proc_binned'] == bin_label],
            legendgroup='Improve',
            scalegroup='Improve',
            name='Improve',
            side='negative',
            line_color='green',
            showlegend=showlegend
        ))
        fig.add_trace(go.Violin(
            x=df_n['proc_binned'][df_n['proc_binned'] == bin_label],
            y=df_n['wait_delta'][df_n['proc_binned'] == bin_label],
            legendgroup='Degrade',
            scalegroup='Degrade',
            name='Degrade',
            side='positive',
            line_color='red',
            showlegend=showlegend
        ))
        showlegend = False

    fig.update_traces(meanline_visible=True)
    fig.update_layout(violingap=0, violinmode='overlay')

    

    figs['wait_delta_vs_proc'] = {
        'fig' : fig,
        'md' : """
By how much do they degrade/improve by?
"""
    }


    # Figure bar plot job count vs runtime bins for positive delta vs negative delta
    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    totals = []


    for _df, name in zip([df_p, df_n, df_0], ['Improve', 'Degrade', 'Unchanged']):
        _df['walltime_binned'] = pd.cut(_df['walltime'], bins=bins, labels=labels, right=True)
        category_counts = _df['walltime_binned'].value_counts()
        values = []
        # fig.add_trace(
        #     go.Bar(
        #         x=category_counts.index,
        #         y=category_counts.values,
        #         name=name
        #     )
        # )
        fig.add_trace(go.Histogram(
            x=category_counts.index,
            y=category_counts.values,
            histfunc='sum',
            name=name,
            texttemplate='%{y:.1f}%',
            textfont_size=10,
            xbins=dict(size=1)
        ))
    # fig.update_layout(barmode='stack', xaxis={'categoryorder':'array', 'categoryarray':labels})
    fig.update_layout(
        barmode='relative',
        barnorm='percent',
        title='Job Count % vs Walltime (mins)',
        xaxis_title='Walltime (mins)',
        yaxis_title='Job Count (%)',
        yaxis=dict(
            tickformat='.0%',
            tickvals=[i for i in range(0, 110, 10)],
            ticktext=['{:.1f}%'.format(i) for i in range(0, 110, 10)])
    )

    figs['counts_vs_walltime'] = {
        'fig' : fig,
        'md' : """
Which kind of jobs improve, degrade?
"""

    }

    # Figure violin plot for delta wait time vs runtime bin
    fig = go.Figure()
    showlegend = True
    for bin_label in labels:
        fig.add_trace(go.Violin(
            x=df_p['walltime_binned'][df_p['walltime_binned'] == bin_label],
            y=df_p['wait_delta'][df_p['walltime_binned'] == bin_label],
            legendgroup='Improve',
            scalegroup='Improve',
            name='Improve',
            side='negative',
            line_color='green',
            showlegend=showlegend
        ))
        fig.add_trace(go.Violin(
            x=df_n['walltime_binned'][df_n['walltime_binned'] == bin_label],
            y=df_n['wait_delta'][df_n['walltime_binned'] == bin_label],
            legendgroup='Degrade',
            scalegroup='Degrade',
            name='Degrade',
            side='positive',
            line_color='red',
            showlegend=showlegend
        ))
        showlegend = False

    fig.update_traces(meanline_visible=True)
    fig.update_layout(violingap=0, violinmode='overlay')


    figs['wait_delta_vs_runtime'] = {
        'fig' : fig,
        'md' : """
By how much do they degrade/improve?
"""
    }



    return figs

def build_figures_2(
    cluster_1,
    cluster_2,
    cluster_3,
    cluster_4,
    start = -1,
    end = -1
        
):
    figs = {}
    start_time = 1641021254

    # Optimal turnaround
    c1 = read_rst(cluster_1)
    c2 = read_rst(cluster_2)
    c3 = read_rst(cluster_3)
    c4 = read_rst(cluster_4)
    t= pd.concat([c3, c4], axis=0)
    
        

    # Figure for wait time delta vs submit time
    fig = go.Figure()

    merged_c1 = pd.merge(c1, t[['id', 'wait']], on='id', how='left', suffixes=('', '_t'))
    merged_c2 = pd.merge(c2, t[['id', 'wait']], on='id', how='left', suffixes=('', '_t'))

    merged_c1['wait_delta'] = merged_c1['wait_t'] - merged_c1['wait']
    merged_c2['wait_delta'] = merged_c2['wait_t'] - merged_c2['wait']

    c1['wait_delta'] = merged_c1['wait_delta']/60
    c2['wait_delta'] = merged_c2['wait_delta']/60

    c1['realtime'] = c1['submit'] + start_time
    c2['realtime'] = c2['submit'] + start_time

    c1['datetime'] = pd.to_datetime(c1['realtime'], unit='s')
    c2['datetime'] = pd.to_datetime(c2['realtime'], unit='s')

    c1_nz =  c1[c1['wait_delta'] != 0]
    c2_nz =  c2[c2['wait_delta'] != 0]

    fig.add_trace(go.Scatter(x=c1_nz['datetime'], y=c1_nz['wait_delta'],
                    mode='markers',
                    name='cluster 1'))
    
    fig.add_trace(go.Scatter(x=c2_nz['datetime'], y=c2_nz['wait_delta'],
                mode='markers',
                name='cluster 2'))
    fig.update_layout(
        title='Wait time (theta) - Wait time (cluster X) vs Submit Time',
        xaxis_title='Submit Time',
        yaxis_title='Delta Wait Time (mins)',
    )

    figs['wait_delta_vs_submit'] = {
        'fig' : fig,
        'md' : """
Do jobs overall improve or degrade?
"""

    }
    # Figure bar plot job count vs proc count bins for positive delta vs negative delta
    fig = go.Figure()
    
    all = pd.concat([c1, c2], axis=0)
    all['walltime'] = all['walltime']/60

    df_p = all[all['wait_delta'] > 0]
    df_n = all[all['wait_delta'] < 0]
    df_0 = all[all['wait_delta'] == 0]
    df_p_all = df_p.copy()
    df_n_all = df_n.copy()

    df_p_all['proc_binned'] = 'Overall'
    df_n_all['proc_binned'] = 'Overall'

    bins = [0, 128, 256, 512, 1024, 2048, float('inf')]
    labels = ['(0, 128]', '(128, 256]','(256, 512]','(512, 1024]', '(1024, 2048]', '(2048, 2180]']
    for _df, name in zip([df_p, df_n, df_0], ['Improve', 'Degrade', 'Unchanged']):
        _df['proc_binned'] =pd.cut(_df['proc1'], bins=bins, labels=labels, right=True)
        category_counts = _df['proc_binned'].value_counts()
        fig.add_trace(
            go.Bar(
                x=category_counts.index,
                y=category_counts.values,
                name=name
            )
        )
        # fig.add_trace(go.Histogram(
        #     x=category_counts.index,
        #     y=category_counts.values,
        #     histfunc='sum',
        #     name=name,
        #     texttemplate='%{y:.2f}%',
        #     textfont_size=10,
        #     xbins=dict(size=1)
        # ))
    fig.update_layout(barmode='stack', xaxis={'categoryorder':'array', 'categoryarray':labels})
    # fig.update_layout(
    #     barmode='relative',
    #     barnorm='percent',
    #     title='Node Count vs Job Count (%)',
    #     xaxis_title='Node Count',
    #     yaxis_title='Job Count (%)',
    #     yaxis=dict(
    #         tickformat='.0%',
    #         tickvals=[i for i in range(0, 110, 10)],
    #         ticktext=['{:.1f}%'.format(i) for i in range(0, 110, 10)])
    # )



    figs['counts_vs_proc'] = {
        'fig' : fig,
        'md' : """
Which kind of jobs improve, degrade?
"""

    }

    # Figure violin plot for delta wait time vs proc count
    all['proc_binned'] =pd.cut(all['proc1'], bins=bins, labels=labels, right=True)
    all_cpy = all.copy()
    all_cpy['proc_binned'] = 'Overall'

    fig = go.Figure()
    showlegend = True
    for bin_label in labels + ['Overall']:
        fig.add_trace(go.Violin(
            x=all['proc_binned'][all['proc_binned'] == bin_label],
            y=all['wait_delta'][all['proc_binned'] == bin_label],
            line_color='blue',
            showlegend=showlegend
        ))

        showlegend = False

        if bin_label == 'Overall':
            fig.add_trace(go.Violin(
                x=all_cpy['proc_binned'],
                y=all_cpy['wait_delta'],
                line_color='blue',
                showlegend=showlegend
            ))

    fig.update_traces(meanline_visible=True)
    fig.update_layout(violingap=0, violinmode='overlay')

    

    figs['wait_delta_vs_proc'] = {
        'fig' : fig,
        'md' : """
By how much do they degrade/improve by?
"""
    }


    # Figure bar plot job count vs runtime bins for positive delta vs negative delta
    fig = go.Figure()
    bins = [0, 10, 30, 60, 120, 250, 500, 1000, 1500, float('inf')]
    labels = ['(0, 10]', '(10, 30]', '(30, 60]', '(60, 120]', '(120, 250]', '(250, 500]', '(500, 1000]', '(1000, 1500]', '(1500, max]']
    totals = []


    for _df, name in zip([df_p, df_n, df_0], ['Improve', 'Degrade', 'Unchanged']):
        _df['walltime_binned'] = pd.cut(_df['walltime'], bins=bins, labels=labels, right=True)
        category_counts = _df['walltime_binned'].value_counts()
        values = []
        fig.add_trace(
            go.Bar(
                x=category_counts.index,
                y=category_counts.values,
                name=name
            )
        )
        # fig.add_trace(go.Histogram(
        #     x=category_counts.index,
        #     y=category_counts.values,
        #     histfunc='sum',
        #     name=name,
        #     texttemplate='%{y:.1f}%',
        #     textfont_size=10,
        #     xbins=dict(size=1)
        # ))
    fig.update_layout(barmode='stack', xaxis={'categoryorder':'array', 'categoryarray':labels})
    # fig.update_layout(
    #     barmode='relative',
    #     barnorm='percent',
    #     title='Job Count % vs Walltime (mins)',
    #     xaxis_title='Walltime (mins)',
    #     yaxis_title='Job Count (%)',
    #     yaxis=dict(
    #         tickformat='.0%',
    #         tickvals=[i for i in range(0, 110, 10)],
    #         ticktext=['{:.1f}%'.format(i) for i in range(0, 110, 10)])
    # )

    figs['counts_vs_walltime'] = {
        'fig' : fig,
        'md' : """
Which kind of jobs improve, degrade?
"""

    }

    # Figure violin plot for delta wait time vs runtime bin
    fig = go.Figure()
    showlegend = True
    for bin_label in labels + ['Overall']:
        fig.add_trace(go.Violin(
            x=df_p['walltime_binned'][df_p['walltime_binned'] == bin_label],
            y=df_p['wait_delta'][df_p['walltime_binned'] == bin_label],
            legendgroup='Improve',
            scalegroup='Improve',
            name='Improve',
            side='negative',
            line_color='green',
            showlegend=showlegend
        ))
        fig.add_trace(go.Violin(
            x=df_n['walltime_binned'][df_n['walltime_binned'] == bin_label],
            y=df_n['wait_delta'][df_n['walltime_binned'] == bin_label],
            legendgroup='Degrade',
            scalegroup='Degrade',
            name='Degrade',
            side='positive',
            line_color='red',
            showlegend=showlegend
        ))
        showlegend = False
        if bin_label == 'Overall':
            fig.add_trace(go.Violin(
                x=df_p_all['proc_binned'],
                y=df_p_all['wait_delta'],
                legendgroup='Improve',
                scalegroup='Improve',
                name='Improve',
                side='negative',
                line_color='blue'
            ))
            fig.add_trace(go.Violin(
                x=df_n_all['proc_binned'],
                y=df_n_all['wait_delta'],
                legendgroup='Degrade',
                scalegroup='Degrade',
                name='Degrade',
                side='positive',
                line_color='orange'
            ))

    fig.update_traces(meanline_visible=True)
    fig.update_layout(violingap=0, violinmode='overlay')


    figs['wait_delta_vs_runtime'] = {
        'fig' : fig,
        'md' : """
By how much do they degrade/improve?
"""
    }

    return figs


def homogeneous_break_down(start = -1, end = -1):
    '''
    
    '''

    
    figs_opt = build_figures(
        cluster_1 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
        cluster_2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
        theta = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/theta/Results/theta_2022.rst',
        start=start,
        end = end
    )

    figs_random = build_figures(
        cluster_1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
        cluster_2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
        theta = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/theta/Results/theta_2022.rst',
        start=start,
        end = end
    )

    return {
        "turnaround" : figs_opt,
        "random" : figs_random
    }


def homogeneous_break_down_nt(start = -1, end = -1):
    '''
    
    '''

    return {
        f'turnaround_v_random' : build_figures_2(
        cluster_1 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
        cluster_2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
        cluster_3 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
        cluster_4 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
            start=start,
            end = end
        )
    }


def heterogeneous_break_down(speed, user_prob, start = -1, end = -1):
    return {
        f'turnaround {speed}' : build_figures(
            cluster_1 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_{speed}/cluster_1/Results/theta_2022.rst',
            cluster_2 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_{speed}/cluster_2/Results/theta_2022.rst',
            theta = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_{speed}/theta/Results/theta_2022.rst',
            start=start,
            end = end
        ),
        f'user {speed} {user_prob}' : build_figures(
            cluster_1 = f'../data/cloudlab/exp_theta_two_parts/probable_user_{speed}_{user_prob}/cluster_1/Results/theta_2022.rst',
            cluster_2 = f'../data/cloudlab/exp_theta_two_parts/probable_user_{speed}_{user_prob}/cluster_2/Results/theta_2022.rst',
            theta = f'../data/cloudlab/exp_theta_two_parts/probable_user_{speed}_{user_prob}/theta/Results/theta_2022.rst',
            start=start,
            end = end
            
        )
    }

def heterogeneous_break_down_nt(speed, user_prob, start = -1, end = -1):
    return {
        f'turnaround {speed}' : build_figures_2(
            cluster_1 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_{speed}/cluster_1/Results/theta_2022.rst',
            cluster_2 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_{speed}/cluster_2/Results/theta_2022.rst',
            cluster_3 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
            cluster_4 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
            start=start,
            end = end
        ),
        f'user {speed} {user_prob}' : build_figures_2(
            cluster_1 = f'../data/cloudlab/exp_theta_two_parts/probable_user_{speed}_{user_prob}/cluster_1/Results/theta_2022.rst',
            cluster_2 = f'../data/cloudlab/exp_theta_two_parts/probable_user_{speed}_{user_prob}/cluster_2/Results/theta_2022.rst',
            cluster_3 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
            cluster_4 = f'../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
            start=start,
            end = end
            
        )
    }