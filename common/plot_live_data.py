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
                        'name': f'Jagriti-{self.itr}', 'company': 'micro', 'hostel': 'ff21', 'itr': self.itr
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
