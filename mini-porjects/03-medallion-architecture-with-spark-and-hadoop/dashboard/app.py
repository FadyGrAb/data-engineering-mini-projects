import io

import pandas as pd
import plotly.graph_objects as go
from flask import Flask
from hdfs import InsecureClient
from plotly.subplots import make_subplots

app = Flask(__name__)


def get_hdfs_data(hdfs_path: str) -> pd.DataFrame:
    client = InsecureClient("http://hadoop-namenode:50070")
    files = client.list(hdfs_path)
    files = [file for file in files if ".csv" in file]
    dfs = []
    for file in files:
        file_hdfs_path = f"{hdfs_path}/{file}"
        with client.read(file_hdfs_path) as reader:
            df = pd.read_csv(io.BytesIO(reader.read()))
        dfs.append(df)
    return pd.concat(dfs)


@app.route("/", methods=["GET"])
def home():

    deliveries_count_df = get_hdfs_data("/data/gold/deliveriescount")
    deliveries_avg_df = get_hdfs_data("/data/gold/deliveriesavg")
    deliveries_count_df.sort_values(by="deliveries_count", ascending=True, inplace=True)
    deliveries_avg_df.sort_values(by="avg_deliverytime", ascending=False, inplace=True)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=[
            "Total deliveries per branch",
            "Average delivery time in hours per branch",
        ],
    )

    fig.add_trace(
        go.Bar(
            name="Deliveries Count",
            x=deliveries_count_df["deliveries_count"],
            y=deliveries_count_df["branch"],
            orientation="h",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            name="Average delivery time",
            x=deliveries_avg_df["avg_deliverytime"],
            y=deliveries_avg_df["branch"],
            orientation="h",
        ),
        row=1,
        col=2,
    )
    fig.update_xaxes(title_text="Delivery Count", row=1, col=1)
    fig.update_xaxes(title_text="Average Time (Hours)", row=1, col=2)

    fig.update_layout(autosize=False, width=2000, height=700)

    return f"<H1>Branches Performance</H1>{fig.to_html()}"
