# Import packages
from dash import Dash, html, dash_table, dcc
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.express import data




def figure_wait_vs_submit(data):

    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']
    dfs = []
    for label in data:
        for file_path in data[label]:
            df = pd.read_csv(file_path, sep=';', header=None) 
            df.columns = column_names
            df['label'] = label
            dfs.append(df)
    return px.scatter(
        pd.concat(dfs, ignore_index=True), 
        x='submit', 
        y='wait',
        color='label',
        hover_data=['id','run', 'proc1']
    )

def figures_heatmap(data):

    column_names = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']

    df = pd.read_csv(data, sep=';', header=None) 
    df.columns = column_names
    df['walltime-days'] = df['walltime']/3600.0

    df['proc_category'] = df['proc1'].apply(lambda x: 'S' if x < 21 else 
                                ('M1' if 21 <= x < 61 else
                                 'L0' if 61 <= x < 250 else
                                 ('L' if 251 <= x < 550 else 'XL')))




    # fig = px.density_heatmap(df, x="x_binned", y="y_binned", marginal_x="histogram", marginal_y="histogram", text_auto=True)\
    fig = px.histogram(df, x="proc_category")
    # fig.update_traces(
    #     xbins=dict(start=min(df.proc1),end=300,size=25),
    #     ybins = dict(start=min(df.walltime), end= 10000, size=1000)
    # )
    return fig
    

# Initialize the app
app = Dash()

# App layout
# Random vs Shortest Job Turnaround Plots
# app.layout = [
#     html.Div(children=[
#         html.P('Wait time vs Submit Timestamp'),
#         dcc.Graph(figure=figure_wait_vs_submit(
#             data = {
#                 'random' : [
#                     '../data/experiments/exp_random/cluster_0_2180_100.rst',
#                     '../data/experiments/exp_random/cluster_1_2180_100.rst'
#                 ],
#                 'homogenous' : [
#                     '../data/experiments/exp_homo_st/cluster_0_2180_100.rst',
#                     '../data/experiments/exp_homo_st/cluster_1_2180_100.rst'
#                 ],
#                 'theta' : [
#                     '../data/experiments/theta_2022_1K/theta_1000.rst'
#                 ],
#             })
#         )
#     ]),
#     html.Div(children=[
#         html.Div(children=[
#             html.P('Homogeneous cluster 0 - Random Assignment'),
#         dcc.Graph(id="graph1", style={'display': 'inline-block'}, figure=figures_heatmap(
#             '../data/experiments/exp_random/cluster_0_2180_100.rst'
#         )),
#         ], style={'width': '50%', 'display': 'inline-block'}),
#         html.Div(children=[
#             html.P('Homogeneous cluster 1 - Random Assignmet'),
#         dcc.Graph(id="graph2", style={'display': 'inline-block'}, figure=figures_heatmap(
#             '../data/experiments/exp_random/cluster_1_2180_100.rst'
#         ))
#         ], style={'width': '50%', 'display': 'inline-block'}),
#     ]),
#     html.Div(children=[
#         html.Div(children=[
#             html.P('Homogeneous cluster 0 - Shortest turnover'),
#         dcc.Graph(id="graph1", style={'display': 'inline-block'}, figure=figures_heatmap(
#             '../data/experiments/exp_homo_st/cluster_0_2180_100.rst'
#         )),
#         ], style={'width': '50%', 'display': 'inline-block'}),
#         html.Div(children=[
#             html.P('Homogeneous cluster 1 - Shortest turnover'),
#         dcc.Graph(id="graph2", style={'display': 'inline-block'}, figure=figures_heatmap(
#             '../data/experiments/exp_homo_st/cluster_1_2180_100.rst'
#         ))
#         ], style={'width': '50%', 'display': 'inline-block'}),
#     ]),
# ]

result_dir = '../data/Results'

# Exp 1 results
exp1_dir = result_dir + '/probable_user_125_60'
exp1_result_files = [
    exp1_dir + '/Results/theta_2022_0.rst',
    exp1_dir + '/Results/theta_2022_1.rst'
]

# Exp 2 results
exp2_dir = result_dir + '/optimal_turnaround_125'
exp2_result_files = [
    exp2_dir + '/Results/theta_2022_0.rst',
    exp2_dir + '/Results/theta_2022_1.rst'
]


app.layout = [
    html.Div(children=[
        html.P('Wait time vs Submit Timestamp'),
        dcc.Graph(figure=figure_wait_vs_submit(
            data = {
                'Prob User 60 1x 1.25x' : [
                    exp1_result_files[0],
                    exp1_result_files[1]
                ],
                'OPT 1x 1.25xx' : [
                    exp2_result_files[0],
                    exp2_result_files[1]
                ],
                'theta original' : [
                    '../data/experiments/theta_2022_1K/theta_1000.rst'
                ],
            })
        )
    ]),
    # html.Div(children=[
    #     html.Div(children=[
    #         html.P('Het cluster 0  1.5x Shortest Turnaround'),
    #     dcc.Graph(style={'display': 'inline-block'}, figure=figures_heatmap(
    #         '../data/experiments/exp_homo_st_slow/cluster_0_2180_150.rst',
    #     )),
    #     ], style={'width': '50%', 'display': 'inline-block'}),
    #     html.Div(children=[
    #         html.P('Het cluster 1  1x Shortest Turnaround'),
    #     dcc.Graph(style={'display': 'inline-block'}, figure=figures_heatmap(
    #         '../data/experiments/exp_homo_st_slow/cluster_1_2180_100.rst'
    #     ))
    #     ], style={'width': '50%', 'display': 'inline-block'}),
    # ]),
    # html.Div(children=[
    #     html.Div(children=[
    #         html.P('Homogeneous cluster 0 - Shortest turnover'),
    #     dcc.Graph(style={'display': 'inline-block'}, figure=figures_heatmap(
    #         '../data/experiments/exp_homo_st/cluster_0_2180_100.rst'
    #     )),
    #     ], style={'width': '50%', 'display': 'inline-block'}),
    #     html.Div(children=[
    #         html.P('Homogeneous cluster 1 - Shortest turnover'),
    #     dcc.Graph( style={'display': 'inline-block'}, figure=figures_heatmap(
    #         '../data/experiments/exp_homo_st/cluster_1_2180_100.rst'
    #     ))
    #     ], style={'width': '50%', 'display': 'inline-block'}),
    # ]),
]

# Run the app
if __name__ == '__main__':
    app.run(debug=True)