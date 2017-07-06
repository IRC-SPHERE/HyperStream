from IPython.core.display import display, HTML
import random

def plot_high_chart(time, data, title="title", xax="X", yax="Y", type="high_chart"):
    if len(time) != len(data):
        print "Length of time and data must agree"
        return None

    if type == "high_chart":
        a = "Highcharts.chart"
        b = "type: 'linear'"
    else:
        a = "Highcharts.stockChart"
        b = "type: 'datetime', ordinal: false"

    r = str(random.randint(0,10000))

    template = """
        <html>
        <head>
        <title>%s</title>
           <!--<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>-->
           <!--<script src="https://code.highcharts.com/stock/highstock.js"></script>-->
           <!--<script src="https://code.highcharts.com/stock/modules/exporting.js"></script>-->
           <script src="./scripts/jquery.min.js"></script>
           <script src="./scripts/highstock.js"></script>
           <script src="./scripts/exporting.js"></script>
        </head>
        <body>

        <div id="container%s" style="width: 800px; height: 600px; margin: 125 auto"></div>

        <script language="JavaScript">
               var data = %s;

            %s('container%s', {
                chart: {
                    zoomType: 'x'
                },
                title: {
                    text: '%s'
                },
                subtitle: {
                    text: document.ontouchstart === undefined ?
                            'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
                },
                xAxis: {
                    %s

                },
                yAxis: {
                    title: {
                        text: '%s'
                    }
                },
                legend: {
                    enabled: false
                },

                series: [{
                    type: 'spline',
                    name: '%s',
                    data: data
                }]
            });
        </script>

        </body>
        </html>

    """

    d = [[i,j] for i, j in zip(time, data)]
    d = str(d)
    f = template % (title, r, d, a, r, title, b, yax, xax)

    display(HTML(f))
