import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import psycopg2
import pandas as pd
import threading
import time

# Global store for the refreshed data
data_store = {}

# Database connection parameters
db_params = {
    "dbname": "beatbnk_db",
    "user": "user",
    "password": "X1SOrzeSrk",
    "host": "beatbnk-db-green-0j3yjq.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
    "port": "5432"
}

tables_to_query = [
    "attendees", "categories", "event_tickets", "events", "follows", "genres",
    "mpesa_stk_push_payments", "performer_tips", "performers",
    "users", "venues"
]

# Refresh data from DB every 60 seconds
def fetch_data():
    global data_store
    while True:
        try:
            new_data_store = {}
            conn = psycopg2.connect(**db_params)
            for table in tables_to_query:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                new_data_store[table] = df
            conn.close()
            data_store = new_data_store
            print("✅ Data refreshed from DB at", time.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            print(f"Error fetching data: {e}")
        time.sleep(60)

# Start background thread for refreshing data
threading.Thread(target=fetch_data, daemon=True).start()

# Dash app
app = dash.Dash(__name__)
app.title = "BeatBnk Analytics Dashboard"

server = app.server

app.layout = html.Div([
    html.H1("BeatBnk Data Dashboard", style={"textAlign": "center"}),
    dcc.Tabs([
        dcc.Tab(label='Events Overview', children=[
            html.Div(id='events-content')
        ]),
        dcc.Tab(label='User Engagement', children=[
            html.Div(id='users-content')
        ]),
        dcc.Tab(label='Payments & Tips', children=[
            html.Div(id='payments-content')
        ])
    ]),
    dcc.Interval(id='interval-refresh', interval=60*1000, n_intervals=0)
])

# Event Overview callback
@app.callback(Output('events-content', 'children'), Input('interval-refresh', 'n_intervals'))
def update_events_tab(n):
    if 'events' not in data_store or 'event_tickets' not in data_store:
        return html.P("Loading events data...")

    events_df = data_store['events']
    tickets_df = data_store['event_tickets']

    if events_df.empty:
        return html.P("No events available.")

    fig_events = px.histogram(events_df, x='eventDate', title='Events Over Time')
    fig_tickets = px.box(tickets_df, x='ticketType', y='price', title='Ticket Prices by Type')

    return html.Div([
        html.P(f"Last refreshed at {time.strftime('%H:%M:%S')}"),
        dcc.Graph(figure=fig_events),
        dcc.Graph(figure=fig_tickets)
    ])

# User Engagement callback
@app.callback(Output('users-content', 'children'), Input('interval-refresh', 'n_intervals'))
def update_users_tab(n):
    if 'attendees' not in data_store or 'follows' not in data_store:
        return html.P("Loading user engagement data...")

    attendees_df = data_store['attendees']
    follows_df = data_store['follows']

    fig_attendees = px.histogram(attendees_df, x='eventId', title='Event Attendance Distribution')
    fig_follows = px.histogram(follows_df, x='entityType', title='Follow Types Distribution')

    return html.Div([
        html.P(f"Last refreshed at {time.strftime('%H:%M:%S')}"),
        dcc.Graph(figure=fig_attendees),
        dcc.Graph(figure=fig_follows)
    ])

# Payments & Tips callback
@app.callback(Output('payments-content', 'children'), Input('interval-refresh', 'n_intervals'))
def update_payments_tab(n):
    if 'mpesa_stk_push_payments' not in data_store or 'performer_tips' not in data_store:
        return html.P("Loading payment data...")

    pay_df = data_store['mpesa_stk_push_payments']
    tips_df = data_store['performer_tips']

    fig_payments = px.histogram(pay_df, x='transactionAmount', nbins=30, title='Payment Amount Distribution')
    fig_tips = px.histogram(tips_df, x='tipAmount', nbins=30, title='Performer Tip Amounts')

    return html.Div([
        html.P(f"Last refreshed at {time.strftime('%H:%M:%S')}"),
        dcc.Graph(figure=fig_payments),
        dcc.Graph(figure=fig_tips)
    ])

if __name__ == '__main__':
    app.run(debug=True, port=8050)

