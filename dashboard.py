import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import time
import datetime

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Crypto Sentinel | Pro",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS FOR "FANCY" LOOK ---
st.markdown("""
<style>
    .metric-card {
        background-color: #0E1117;
        border: 1px solid #262730;
        border-radius: 5px;
        padding: 15px;
        color: white;
    }
    .stMetric {
        background-color: transparent !important;
    }
    /* Pulsing Green Dot for Live Status */
    .live-indicator {
        height: 10px;
        width: 10px;
        background-color: #00FF00;
        border-radius: 50%;
        display: inline-block;
        box-shadow: 0 0 5px #00FF00;
    }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.title("🦅 Sentinel Controls")
    st.markdown("---")
    
    # 1. Time Window Filter
    time_window = st.selectbox(
        "⏱️ Time Window",
        options=[100, 500, 1000, 5000],
        format_func=lambda x: f"Last {x} Trades",
        index=1
    )
    
    # 2. Refresh Rate
    refresh_rate = st.slider("⚡ Refresh Rate (sec)", 0.5, 5.0, 1.0)
    
    st.markdown("---")
    st.markdown("### 📡 Data Source")
    st.info("**Coinbase Pro WebSocket**\n\nDirect Feed: `BTC-USD`\n\nLatency: < 50ms")
    
    if st.button("🧹 Clear Cache"):
        st.cache_data.clear()

# --- MAIN DASHBOARD LAYOUT ---
st.markdown('## <span class="live-indicator"></span> Crypto Sentinel <span style="color:gray; font-size:0.6em">Real-Time Lakehouse</span>', unsafe_allow_html=True)

placeholder = st.empty()

while True:
    try:
        # --- 1. QUERY THE DATA LAKE (DuckDB) ---
        query = f"""
        SELECT 
            timestamp, 
            price, 
            symbol,
            processing_time
        FROM read_parquet('spark_data/storage/*.parquet')
        ORDER BY timestamp DESC
        LIMIT {time_window}
        """
        
        # Load Data
        df = duckdb.query(query).to_df()
        
        if df.empty:
            st.warning("⏳ Waiting for data lake to populate...")
            time.sleep(2)
            continue

        # Convert timestamp to datetime for plotting
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp') # Sort for the chart

        # --- 2. CALCULATE METRICS ---
        latest_price = df.iloc[-1]['price']
        start_price = df.iloc[0]['price']
        price_change = latest_price - start_price
        pct_change = (price_change / start_price) * 100
        
        session_high = df['price'].max()
        session_low = df['price'].min()
        volatility = df['price'].std()

        # Determine Color Scheme based on trend
        chart_color = '#00FF00' if price_change >= 0 else '#FF0000'

        # --- 3. RENDER UI ---
        with placeholder.container():
            
            # --- ROW 1: KEY METRICS ---
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Bitcoin (BTC)", f"${latest_price:,.2f}", f"{pct_change:.2f}%")
            with col2:
                st.metric("Session High", f"${session_high:,.2f}")
            with col3:
                st.metric("Session Low", f"${session_low:,.2f}")
            with col4:
                st.metric("Volatility (StdDev)", f"${volatility:.2f}")

            # --- ROW 2: PROFESSIONAL CHART ---
            # We use Plotly Graph Objects for a "Financial" look
            fig = go.Figure()
            
            # Main Area Chart
            fig.add_trace(go.Scatter(
                x=df['timestamp'], 
                y=df['price'],
                mode='lines',
                name='Price',
                line=dict(color=chart_color, width=2),
                fill='tozeroy', # <--- This was the culprit!
                fillcolor=f"rgba({0 if price_change >= 0 else 255}, {255 if price_change >= 0 else 0}, 0, 0.1)"
            ))

            # DYNAMIC Y-AXIS CALCULATION
            # We add a tiny buffer (0.1%) above and below the min/max prices
            # so the line doesn't hit the absolute edge of the chart.
            y_min = df['price'].min() * 0.999
            y_max = df['price'].max() * 1.001

            fig.update_layout(
                title=f"BTC-USD Price Action ({time_window} ticks)",
                xaxis_title="Time (UTC)",
                yaxis_title="Price (USD)",
                yaxis_range=[y_min, y_max],  # <--- FORCE THE ZOOM
                template="plotly_dark",
                height=500,
                margin=dict(l=0, r=0, t=40, b=0),
                hovermode="x unified"
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # --- ROW 3: LATEST TRADES LOG ---
            st.markdown("### 📝 Latest Executed Trades")
            
            # Create a styled dataframe for the trade log
            latest_trades = df.tail(10).sort_values('timestamp', ascending=False)
            
            # Use columns to make it look like a ticker
            st.dataframe(
                latest_trades[['timestamp', 'price', 'symbol']],
                use_container_width=True,
                hide_index=True
            )

        # Refresh Loop
        time.sleep(refresh_rate)

    except Exception as e:
        with placeholder.container():
            st.error(f"Dashboard Error: {str(e)}")
            time.sleep(5)