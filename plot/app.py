from dash import Dash, dcc, html
from exp_theta_two_parts import \
    violin_cmp_2_exp_wait_v_node_count, \
    violin_cmp_2_exp_wait_v_walltime, \
    violin_cmp_2_exp_boslo_v_node_count, \
    violin_cmp_2_exp_boslo_v_walltime, \
    violin_cmp_2_exp_boslo_v_node_count_95, \
    violin_cmp_2_exp_boslo_v_walltime_95
from dash import Dash, html, dcc, Input, Output, callback



app = Dash()
# figs = homogeneous_break_down()
# figs_het = heterogeneous_break_down(1.3, 0.5)
# figs_het2 = heterogeneous_break_down(1.1, 0.5)
app.layout = html.Div([
    dcc.RangeSlider(0, 12, 0.5, value=[0, 12], id='my-range-slider'),
    html.Div(id='graphs'),

])

@callback(
    Output('graphs', 'children'),
    Input('my-range-slider', 'value'))
def update_output(value):
    start = 0
    end = 31500000

    # start_c = ((end - start)/12)*value[0]
    # end_c = ((end - start)/12)*value[1]
    # figs = homogeneous_break_down(start=start_c, end=end_c)
    # figs_hom_nt = homogeneous_break_down_nt(start=start_c, end=end_c)
    # figs_het = heterogeneous_break_down(1.3, 0.5, start=start_c, end=end_c)
    # figs_het_nt = heterogeneous_break_down_nt(1.3, 0.5, start=start_c, end=end_c)
    # figs_het2 = heterogeneous_break_down(1.1, 0.5, start=start_c, end=end_c)
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
            *violin_cmp_2_exp_boslo_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_node_count_95(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_walltime_95(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            )
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
            *violin_cmp_2_exp_boslo_v_node_count(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_walltime(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_node_count_95(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            ),
            *violin_cmp_2_exp_boslo_v_walltime_95(
                exp1c1 ='../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_1/Results/theta_2022.rst',
                exp1c2 = '../data/cloudlab/exp_theta_two_parts/optimal_turnaround_1.3/cluster_2/Results/theta_2022.rst',
                exp2c1 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_1/Results/theta_2022.rst',
                exp2c2 = '../data/cloudlab/exp_theta_two_parts/probable_user_1.3_0.5/cluster_2/Results/theta_2022.rst',
                exp_1_name = "SGS-T",
                exp_2_name = "Random"
            )
        ]),

    ],  id="my-tabs")

if __name__ == '__main__':
    app.run(debug=True)