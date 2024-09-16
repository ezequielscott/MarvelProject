from dash import Dash, dash_table, html
import pandas as pd

filename = "data/characters.csv"

with open(filename) as f:
    df = pd.read_csv(f)

# Thumbnail following markdown format: [![alt text](image link)](web link)
df["thumbnail"] = df.apply( lambda x : '[<img src="' + x['img'] + '" width="100" />](' + x['img'] + ')', axis=1)

# sort descending
df = df.sort_values("comics", ascending=False)

app = Dash(__name__)

app.layout =  html.Div(
    [
        dash_table.DataTable(
            data=df.to_dict('records'), 
            columns=[
                {"id": "id", "name": "ID"},
                {"id": "name", "name": "Name"},
                {"id": "thumbnail", "name": "Image", "presentation": "markdown"},
                {"id": "comics", "name": "Comics"},
            ],
            markdown_options={"html": True},            
        )
    ]
)

if __name__ == '__main__':
    app.run(debug=True)