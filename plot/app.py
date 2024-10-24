from dash import Dash, dcc, html
from exp_theta_two_parts import \
    violin_cmp_2_exp_wait_v_node_count, \
    violin_cmp_2_exp_wait_v_walltime, \
    violin_cmp_2_exp_boslo_v_node_count, \
    violin_cmp_2_exp_boslo_v_walltime, \
    violin_cmp_2_exp_boslo_v_node_count_95, \
    violin_cmp_2_exp_boslo_v_walltime_95, \
    line_cmp_2_exp_avg_uti_v_time, \
    line_job_submit_events_per_day
from dash import Dash, html, dcc, Input, Output, callback



app = Dash()
app.layout = html.Div([
    dcc.RangeSlider(0, 12, 0.5, value=[0, 12], id='my-range-slider'),
    html.Div(id='graphs'),

])

@callback(
    Output('graphs', 'children'),
    Input('my-range-slider', 'value'))
def update_output(value):
    return dcc.Tabs([
        dcc.Tab(label='SGS-T vs Random (Homogeneous)', children=[
            *violin_cmp_2_exp_wait_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_wait_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            dcc.Markdown(
            f"""
# Per Cluster Comparison
"""
        ),
            dcc.Markdown(
            f"""
## Cluster 1: 2180 nodes
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.ult',
                exp_1_name = "SGS-T",
                exp_2_name = "Random",
                start_time_offset=1641021254
            ),
                     dcc.Markdown(
            f"""
## Cluster 2: 2180 nodes
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T",
                exp_2_name = "Random",
                start_time_offset=1641021254
            ),
            dcc.Markdown(
            f"""
# Per Stratgey Comparison
"""
        ),
            dcc.Markdown(
            f"""
## SGS-T Cluster 1 vs Cluster 2
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T Cluster 1",
                exp_2_name = "SGS-T Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
## Random Cluster 1 vs Cluster 2
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "Random Cluster 1",
                exp_2_name = "Random Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
# Job Submission Events per Cluster
"""
        ),
            dcc.Markdown(
            f"""
## SGS-T Cluster 1 vs Cluster 2
"""     ),
            *line_job_submit_events_per_day(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.ult',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T Cluster 1",
                exp_2_name = "SGS-T Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
## Random Cluster 1 vs Cluster 2
"""
        ),
            *line_job_submit_events_per_day(
                exp1c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.ult',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "Random Cluster 1",
                exp_2_name = "Random Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            
        ]),
        dcc.Tab(label='SGS-T vs Random (Heterogeneous)', children=[
            *violin_cmp_2_exp_wait_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_wait_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
                                dcc.Markdown(
            f"""
# Per Cluster Comparison
"""
        ),
                                dcc.Markdown(
            f"""
## Cluster 1: 2180 nodes
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.ult',
                exp_1_name = "SGS-T",
                exp_2_name = "Random",
                start_time_offset=1641021254
            ),
            dcc.Markdown(
            f"""
## Cluster 2: 2180 nodes 30% slower
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T",
                exp_2_name = "Random",
                start_time_offset=1641021254
            ),
            dcc.Markdown(
            f"""
# Per Stratgey Comparison
"""
        ),
            dcc.Markdown(
            f"""
## SGS-T Cluster 1 vs Cluster 2
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T Cluster 1",
                exp_2_name = "SGS-T Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
## Random Cluster 1 vs Cluster 2
"""
        ),
            *line_cmp_2_exp_avg_uti_v_time(
                exp1c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.ult',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "Random Cluster 1",
                exp_2_name = "Random Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
# Job Submission Events per Cluster
"""
        ),
            dcc.Markdown(
            f"""
## SGS-T Cluster 1 vs Cluster 2
"""     ),
            *line_job_submit_events_per_day(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.ult',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.ult',
                exp_1_name = "SGS-T Cluster 1",
                exp_2_name = "SGS-T Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
            dcc.Markdown(
            f"""
## Random Cluster 1 vs Cluster 2
"""
        ),
            *line_job_submit_events_per_day(
                exp1c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.ult',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.ult',
                exp_1_name = "Random Cluster 1",
                exp_2_name = "Random Cluster 2",
                start_time_offset=1641021254,
                line_colors=['red', 'blue']
            ),
        ]),

    ],  id="my-tabs")

if __name__ == '__main__':
    app.run(debug=True)