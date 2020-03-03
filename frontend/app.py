import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
import json
import pandas as pd
import time


# Dash init
app = dash.Dash()
app.css.append_css(
    {'external_url': 'https://codepen.io/amyoshino/pen/jzXypZ.css'})


# Postgres
conn = psycopg2.connect(host='ec2-3-94-71-208.compute-1.amazonaws.com',
                        database='datanodedb', user='datanode', password='password')
cur = conn.cursor()


location = pd.read_csv("nodes.csv")


app.layout = html.Div([
    html.Div([
        html.Div([
            dcc.Graph(id='graph', style={'margin-top': '20'})], className="six columns"),
        html.Div([
            dcc.Graph(
                id='bar-graph'
            )
        ], className='twelve columns'
        ),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds
            n_intervals=0)
    ], className="row")

], className="ten columns offset-by-one")


@app.callback(Output('graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_map(n):
    """
    Args n: int
    :rtype: dict
    """
    try:
        latest_reading = "select id, start_ts, std_temp, rol_avg_read, num_anomaly, anomaly from anomaly_tbl order by start_ts desc, num_anomaly desc limit 90;"
        df_map = pd.read_sql(latest_reading, conn)
        map_data = df_map.join(location.set_index('id'), on='id').round(3)
        clrred = 'rgb(222,0,0)'
        clrgrn = 'rgb(0,222,0)'

        def SetColor(x):
            if x == True:
                return clrred
            else:
                return clrgrn

        layout = {
            'autosize': True,
            'height': 500,
            'font': dict(color="#191A1A"),
            'titlefont': dict(color="#191A1A", size='18'),
            'margin': {
                'l': 35,
                'r': 35,
                'b': 35,
                't': 45
            },
            'hovermode': "closest",
            'plot_bgcolor': '#fffcfc',
            'paper_bgcolor': '#fffcfc',
            'showlegend': False,
            'legend': dict(font=dict(size=10), orientation='h', x=0, y=1),
            'name': map_data['anomaly'],
            'title': 'Anomalous activity for the last 3 seconds',
            'mapbox': {
                'accesstoken': (
                    'pk.eyJ1IjoiY2hyaWRkeXAiLCJhIjoiY2ozcGI1MTZ3M' +
                    'DBpcTJ3cXR4b3owdDQwaCJ9.8jpMunbKjdq1anXwU5gxIw'
                ),
                'center': {
                    'lon': -87.683396,
                    'lat': 41.768319


                },
                'zoom': 9,
                'style': "dark"
            }
        }

        return {
            "data": [{
                "type": "scattermapbox",
                "lat": list(map_data['lat']),
                "lon": list(map_data['lon']),
                "hoverinfo": "text",
                "hovertext": [["sensor_id: {}  <br>stdDev: {} <br>rolAvg: {} <br>num_anomaly: {} <br>anomaly: {}".format(i, j, k, l, m)]
                              for i, j, k, l, m in zip(map_data['id'], map_data['std_temp'].tolist(), map_data['rol_avg_read'].tolist(), map_data['num_anomaly'].tolist(), map_data['anomaly'].tolist())],
                "mode": "markers",
                "marker": {
                    "size": 10,
                    "opacity": 1,
                    "color": list(map(SetColor, map_data['anomaly']))
                }
            }],
            "layout": layout
        }
    except Exception as e:
        print("Error: Couldn't update map")
        print(e)


@app.callback(Output('bar-graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_bar_graph(n):
     """
    Args n: int
    :rtype: dict
    """
    try:

        tot_anomalies_per_sensor = "select id, sum(num_anomaly) AS tot_anom from anomaly_window_tbl group by id order by tot_anom desc limit 10;"
        df_table = pd.read_sql(tot_anomalies_per_sensor, conn)
        bar_data = df_table.join(location.set_index('id'), on='id').round(2)
        layout = dict(title="Top anomalous sensors since integration",
                      xaxis=dict(
                          type='category',
                          title='Sensor ID'),
                      yaxis=dict(
                          type="log",
                          title='total number of anomaly(log)'))

        data = [dict(
            type="bar",
            x=bar_data['id'].tolist(),
            y=bar_data['tot_anom'].tolist()
        )]

        return {"data": data, "layout": layout}
    except Exception as e:
        print("Couldn't update bar-graph")
        print(e)


if __name__ == '__main__':
    app.run_server(debug=False)
