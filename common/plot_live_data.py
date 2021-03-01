import random

from beartype import beartype

DATA_JSON = {}


class PlotLiveData:
    """
    Plot live time series data using matplotlib
    """
    def __init__(self,
                 streaming_data_function,
                 streaming_data_from = "sql",
                 ):

        self.streaming_data_function = streaming_data_function
        self.streaming_data_from     = streaming_data_from
        self.all_data = None
        self.itr = 0

    def plot_data(self, column_to_plot, interval = 10000, **options):
        """
        Calls the streaming_data_function and plots the live data
        :param column_to_plot: name of column that you want to plot
        :param interval: time interval for which the data is fetched
        :return:
        """
        from datetime import datetime as dt
        import pandas as pd
        import matplotlib.pyplot as plt
        from matplotlib.animation import FuncAnimation
        x_data, y_data = [], []

        def update(frame):
            ss = options.get('ss')
            self.itr += 1
            if ss:
                ss.commit_batch_data([
                    {
                        'name': f'Jagriti-{self.itr}', 'company': 'micro', 'hostel': 'ff21',
                        'itr': self.itr + random.randint(0, 20)
                    }
                ])
            data = self.streaming_data_function()
            print(f"latest data tp plot ->", data)

            if data is not None and isinstance(data, pd.DataFrame) and not data.empty:

                latest_data = data[column_to_plot].iloc[-1]
                print(f"latest data tp plot ->", latest_data)
                y_data.append(latest_data)
                x_data.append(dt.now())
                plt.cla()
                plt.xlabel("Time")
                plt.ylabel(column_to_plot)
                plt.plot(x_data, y_data)

        ani = FuncAnimation(plt.gcf(), update, interval = interval)
        plt.tight_layout()

        plt.show()

    @beartype
    def dash_plot(self,
                  columns_to_plot : (str, list),
                  refresh_rate_sec = 10,
                  **options):
        """
        Plot the data using dash and plotly framework to make a real time dashboard.
        :param refresh_rate_sec:
        :param columns_to_plot:
        :return:
        """
        import datetime
        import json
        import dash
        import dash_core_components as dcc
        import dash_html_components as html
        import plotly
        import pandas as pd
        from dash.dependencies import Input, Output

        if isinstance(columns_to_plot, str):
            columns_to_plot = [columns_to_plot]

        external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

        app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
        app.layout = html.Div(
            html.Div([
                html.H4('Python IoT Live Dashboard'),
                html.Div(id='live-update-text'),
                dcc.Graph(id='live-update-graph'),
                dcc.Interval(
                    id='interval-component',
                    interval=refresh_rate_sec * 1000,  # in milliseconds
                    n_intervals=0
                )
            ])
        )

        data_to_plot = {}
        for col in columns_to_plot:
            data_to_plot[col] = []
        data_to_plot['time'] = []

        @app.callback(Output('live-update-text', 'children'),
                      Input('interval-component', 'n_intervals'))
        def update_metrics(n):
            ss = options.get('ss')
            self.itr += 1
            if ss:
                ss.commit_batch_data([
                    {
                        'name': f'john-{self.itr}', 'company': 'micro', 'hostel': 'ff21',
                        'itr': self.itr + random.randint(0, 20), 'itr_': 1.87 * self.itr + 4 * random.randint(0, 20)
                    }
                ])
                print("sent the data")
            global DATA_JSON
            data = self.streaming_data_function()
            style = {'padding': '5px', 'fontSize': '20px'}
            if isinstance(data, pd.DataFrame) and not data.empty:
                DATA_JSON = json.loads(data.to_json(orient="records"))[-1]
                print(DATA_JSON)
                return [
                    html.Span(f'{col} : {DATA_JSON[col]}', style=style) for col in columns_to_plot
                ]

        # Multiple components can update everytime interval gets fired.
        @app.callback(Output('live-update-graph', 'figure'),
                      Input('interval-component', 'n_intervals'))
        def update_graph_live(n):
            global DATA_JSON

            time = datetime.datetime.now()
            for cols in columns_to_plot:
                data_to_plot[cols].append(DATA_JSON[cols])
            data_to_plot['time'].append(time)

            print(f"latest data tp plot ->", data_to_plot)

            # Create the graph with subplots
            fig = plotly.tools.make_subplots(rows=len(columns_to_plot), cols=1, vertical_spacing=0.2)
            fig['layout']['margin'] = {
                'l': 30, 'r': 10, 'b': 30, 't': 10
            }
            fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}
            for i in range(len(columns_to_plot)):
                fig.add_trace({
                    'x': data_to_plot['time'],
                    'y': data_to_plot[columns_to_plot[i]],
                    'name': columns_to_plot[i],
                    'mode': 'lines+markers',
                    'type': 'scatter'
                }, i+1, 1)

            return fig
        app.run_server(debug=True)