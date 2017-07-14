from IPython.core.display import display, HTML
import random
from dateutil.parser import parse
from datetime import datetime

epoch = datetime.utcfromtimestamp(0).replace(tzinfo=None)

def unix_time_miliseconds(dt):
    return (dt.replace(tzinfo=None) - epoch).total_seconds() * 1000.0


def plot_high_chart(time, data, title="title", yax="Y", type="high_chart"):
    if len(time) != len(data):
        print "Length of time and data must agree"
        return None

    if type == "high_chart":
        type_a = "Highcharts.chart"
        type_b = "type: 'linear'"
    else:
        type_a = "Highcharts.stockChart"
        type_b = "type: 'datetime', ordinal: true"
        time = [unix_time_miliseconds(parse(dt)) for dt in time]

    r = str(random.randint(0,10000))

    # braces braces need to be doubled in the template {{ = { and }} = }
    template = """
        <html>
        <head>
        <title>{0}</title>
           <!--<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>-->
           <!--<script src="https://code.highcharts.com/stock/highstock.js"></script>-->
           <!--<script src="https://code.highcharts.com/stock/modules/exporting.js"></script>-->
           <script src="./scripts/jquery.min.js"></script>
           <script src="./scripts/highstock.js"></script>
           <script src="./scripts/exporting.js"></script>
        </head>
        <body>

        <div id="container{1}" style="width: 800px; height: 600px; margin: 125 auto"></div>

        <script language="JavaScript">
               var data = {2};

            {3}('container{4}', {{
                chart: {{
                    zoomType: 'x'
                }},
                title: {{
                    text: '{5}'
                }},
                xAxis: {{
                    {6}
                }},
                yAxis: {{
                    title: {{
                        text: '{7}'
                    }}
                }},
                legend: {{
                    enabled: false
                }},

                series: [{{
                    type: 'spline',
                    name: '{7}',
                    data: data
                }}]
            }});
        </script>

        </body>
        </html>
    """

    data = [[i,j] for i, j in zip(time, data)]
    data = str(data)
    f = template.format(title, r, data, type_a, r, title, type_b, yax)

    display(HTML(f))
