import glob
import os
import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import time

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Crypto Sentinel | Pro",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    
    /* Chart Stability */
    .stPlotlyChart { min-height: 550px; }
    
    /* Live Dot Animation */
    .live-indicator {
        height: 12px; width: 12px; background-color: #00FF00;
        border-radius: 50%; display: inline-block;
        box-shadow: 0 0 10px #00FF00; margin-right: 10px;
        animation: pulse 1.5s infinite;
    }
    
    /* Alert Animation (Red Flash) */
    @keyframes flash-red {
        0% { background-color: #440000; border-color: #FF0000; }
        50% { background-color: #220000; border-color: #880000; }
        100% { background-color: #440000; border-color: #FF0000; }
    }
    .alert-banner {
        animation: flash-red 1s infinite;
        padding: 15px;
        border-radius: 8px;
        border: 2px solid red;
        color: #FFffff;
        text-align: center;
        font-size: 20px;
        font-weight: bold;
        margin-bottom: 20px;
    }
    
    @keyframes pulse {
        0% { opacity: 1; box-shadow: 0 0 5px #00FF00; }
        50% { opacity: 0.5; box-shadow: 0 0 15px #00FF00; }
        100% { opacity: 1; box-shadow: 0 0 5px #00FF00; }
    }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.title("🦅 Sentinel Controls")
    st.markdown("---")
    
    # 1. TIME WINDOW
    # Initialize session state for window if not exists
    if 'time_window' not in st.session_state:
        st.session_state.time_window = 500

    time_window = st.selectbox(
        "⏱️ Time Window",
        options=[100, 500, 1000, 5000],
        format_func=lambda x: f"Last {x} Trades",
        index=1
    )
    
    st.markdown("### 🎮 Interaction")
    
    # 2. FREEZE BUTTON
    freeze_mode = st.toggle("⏸️ Freeze Feed", value=False)
    
    # 3. PRICE ALERT
    enable_alert = st.checkbox("🔔 Enable Price Alert")
    target_price = 0.0
    if enable_alert:
        target_price = st.number_input("Target Price ($)", value=98000.0, step=100.0)
    
    st.markdown("---")
    st.info("**Coinbase Pro WebSocket**\n\nDirect Feed: `BTC-USD`")

# --- HEADER ---
st.markdown('## <span class="live-indicator"></span> Crypto Sentinel <span style="color:gray; font-size:0.6em">Live Lakehouse View</span>', unsafe_allow_html=True)

# --- MAIN DASHBOARD LOGIC ---
@st.fragment(run_every=1)
def render_live_dashboard():
    
    # Initialize Session State Variables if they don't exist
    if 'latest_df' not in st.session_state:
        st.session_state.latest_df = pd.DataFrame()
    if 'alert_end_time' not in st.session_state:
        st.session_state.alert_end_time = 0

    # --- 1. DATA FETCHING LOGIC ---
    df = pd.DataFrame()

    if freeze_mode:
        # If Frozen, use the LAST saved dataframe. Do NOT query DuckDB.
        if not st.session_state.latest_df.empty:
            df = st.session_state.latest_df
            st.warning("⏸️ Feed Frozen: Inspecting historical data (Chart is interactive)")
        else:
            st.warning("⏸️ Feed Frozen: No data available yet.")
            return
    else:
        # If NOT Frozen, Fetch new data
        all_files = glob.glob("spark_data/storage/*.parquet")
        valid_files = [f for f in all_files if os.path.getsize(f) > 1000]

        if not valid_files:
            st.warning("⏳ Waiting for data flow... (Spark is warming up)")
            return

        files_sql = str(valid_files).replace('[', '').replace(']', '')
        
        query = f"""
        SELECT timestamp, price, symbol
        FROM read_parquet([{files_sql}])
        ORDER BY timestamp DESC
        LIMIT {time_window}
        """
        
        try:
            df = duckdb.query(query).to_df()
            # SAVE to session state so we can use it when frozen later
            st.session_state.latest_df = df
        except Exception:
            return

    if df.empty:
        return

    # --- 2. DATA PREP ---
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    latest_price = df.iloc[-1]['price']
    start_price = df.iloc[0]['price']
    price_change = latest_price - start_price
    pct_change = (price_change / start_price) * 100 if start_price != 0 else 0
    
    session_high = df['price'].max()
    session_low = df['price'].min()
    volatility = df['price'].std()

    # --- 3. ALERT LOGIC (PERSISTENT) ---
    # Check if we hit the target
    if enable_alert and latest_price >= target_price:
        # Set the "End Time" for the alert to be 2 seconds from now
        st.session_state.alert_end_time = time.time() + 2

    # Check if we are still within the alert window
    is_alert_active = time.time() < st.session_state.alert_end_time

    if is_alert_active:
        st.markdown(f"""
        <div class="alert-banner">
            🚨 PRICE ALERT: ${latest_price:,.2f} HAS HIT TARGET! 🚨
        </div>
        """, unsafe_allow_html=True)
        chart_color = '#FF0000'
        fill_color = 'rgba(255, 0, 0, 0.2)'
    else:
        chart_color = '#00FF00' if price_change >= 0 else '#FF4B4B'
        fill_color = f"rgba({0 if price_change >= 0 else 255}, {255 if price_change >= 0 else 75}, 0, 0.1)"

    # --- 4. RENDER UI ---
    
    # Row 1: Metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Bitcoin (BTC)", f"${latest_price:,.2f}", f"{pct_change:.2f}%")
    col2.metric("Session High", f"${session_high:,.2f}")
    col3.metric("Session Low", f"${session_low:,.2f}")
    col4.metric("Volatility", f"${volatility:.2f}")

    # Row 2: Chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['price'],
        mode='lines', name='Price',
        line=dict(color=chart_color, width=2),
        fill='tozeroy', fillcolor=fill_color
    ))

    y_min = df['price'].min() * 0.999
    y_max = df['price'].max() * 1.001

    fig.update_layout(
        xaxis_title=None, yaxis_title=None,
        yaxis=dict(range=[y_min, y_max], gridcolor='#262730', side='right'),
        xaxis=dict(gridcolor='#262730'),
        template="plotly_dark", height=550,
        margin=dict(l=0, r=50, t=20, b=0),
        hovermode="x unified",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )

    st.plotly_chart(fig, use_container_width=True, key="live_crypto_chart_static")

    # Row 3: Tape
    st.markdown("### 📝 Trade Tape")
    latest_trades = df.tail(10).sort_values('timestamp', ascending=False)
    st.dataframe(
        latest_trades[['timestamp', 'price', 'symbol']],
        use_container_width=True,
        hide_index=True
    )

# --- START THE ENGINE ---
render_live_dashboard()